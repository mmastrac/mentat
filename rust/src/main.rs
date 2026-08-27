//! mentat: minimal Ray replacement for vLLM multi-node serving.
//!
//! One binary, several roles:
//!   mentatd daemon           -- the per-node cluster daemon
//!   mentatd start [...]      -- ray-CLI-compatible agent launcher
//!   mentatd status / stop    -- cluster inspection and kill switch
//! A `ray` symlink keeps the serving entrypoints' `ray start` / `ray status`
//! invocations working unchanged.

mod agent;
mod announce;
mod config;
mod daemon;
mod gpu;
mod http;
mod logfmt;
mod mesh;
mod proto;
mod state;
mod status;

use std::io::BufReader;
use std::net::TcpStream;

use clap::{Parser, Subcommand};

use proto::{read_frame, write_frame, Frame, Msg};

const RAY_COMPAT_VERSION: &str = "2.57.0";

#[derive(Parser)]
#[command(name = "mentatd", version, disable_version_flag = true)]
struct Cli {
    #[arg(long, global = true)]
    version: bool,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the cluster daemon (mentatd).
    Daemon {
        #[arg(long, default_value_t = 6379)]
        port: u16,
        #[arg(long, default_value_t = 6380)]
        http_port: u16,
        #[arg(long)]
        node_ip: Option<String>,
        #[arg(long, default_value = "/tmp/mentat/head.json")]
        head_json: String,
        /// Comma-separated control addresses of the other mentatd instances
        /// (also MENTAT_PEERS). Entries that are this daemon are skipped.
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
    },
    /// ray-compatible: register this container's GPUs with the local daemon.
    Start {
        /// Accepted for ray CLI compatibility; head/worker is no longer a
        /// distinction mentat needs.
        #[arg(long)]
        head: bool,
        /// Run the agent in the foreground (workers `exec` this).
        #[arg(long)]
        block: bool,
        #[arg(long)]
        address: Option<String>,
        #[arg(long)]
        node_ip_address: Option<String>,
        #[arg(long)]
        port: Option<u16>,
        /// Accepted and ignored: mentat has no object store, on purpose.
        #[arg(long)]
        object_store_memory: Option<u64>,
    },
    /// Show cluster state. With a group scope, prints the ray-compatible
    /// `N.0/M.0 GPU` line the entrypoints gate on.
    Status {
        #[arg(long)]
        address: Option<String>,
        #[arg(long)]
        group: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Kill actors (all, or one group's). The manual unstick lever.
    Stop {
        #[arg(long)]
        address: Option<String>,
        #[arg(long)]
        group: Option<String>,
    },
    /// Internal: the detached agent process spawned by `start`.
    #[command(hide = true)]
    InternalAgent {
        #[arg(long)]
        group: String,
        #[arg(long)]
        daemon: String,
    },
}

fn main() {
    let invoked_as_ray = std::env::args()
        .next()
        .map(|a| a.rsplit('/').next().unwrap_or("").starts_with("ray"))
        .unwrap_or(false);

    let cli = Cli::parse();
    if cli.version {
        if invoked_as_ray {
            println!(
                "ray, version {RAY_COMPAT_VERSION} (mentatd {})",
                env!("CARGO_PKG_VERSION")
            );
        } else {
            println!("mentatd {}", env!("CARGO_PKG_VERSION"));
        }
        return;
    }

    match cli.cmd {
        None => {
            eprintln!("usage: mentatd <daemon|start|status|stop> (see --help)");
            std::process::exit(2);
        }
        Some(Cmd::Daemon {
            port,
            http_port,
            node_ip,
            head_json,
            peers,
        }) => {
            let node_ip = node_ip.unwrap_or_else(daemon::default_node_ip);
            let peers = if peers.is_empty() {
                std::env::var("MENTAT_PEERS")
                    .unwrap_or_default()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            } else {
                peers
            };
            if let Err(e) = daemon::run(daemon::DaemonOpts {
                port,
                http_port,
                node_ip,
                head_json,
                peers,
            }) {
                eprintln!("mentatd daemon failed: {e}");
                std::process::exit(1);
            }
        }
        Some(Cmd::Start {
            block,
            address,
            object_store_memory,
            ..
        }) => {
            if object_store_memory.is_some() {
                logfmt::log(
                    "object_store_flag_ignored",
                    &[(
                        "why",
                        "mentat has no object store; unified memory belongs to the model"
                            .to_string(),
                    )],
                );
            }
            let group = group_from_env();
            // Workers pass --address=<head>; agents connect to that daemon
            // directly (no per-node daemon required). MENTAT_DAEMON overrides
            // for tests and for a future local-daemon mesh.
            let daemon_addr = std::env::var("MENTAT_DAEMON")
                .ok()
                .filter(|s| !s.is_empty())
                .or(address.filter(|s| !s.is_empty()))
                .unwrap_or_else(|| "127.0.0.1:6379".to_string());
            if block {
                agent::run(agent::AgentOpts { daemon_addr, group });
            } else {
                // Detach: the entrypoint continues on to `vllm serve`; the
                // agent lives beside it with inherited stdio so actor logs
                // land in the container log.
                let exe = std::env::current_exe().expect("current_exe");
                let child = std::process::Command::new(exe)
                    .args([
                        "internal-agent",
                        "--group",
                        &group,
                        "--daemon",
                        &daemon_addr,
                    ])
                    .spawn();
                match child {
                    Ok(c) => {
                        let _ = state::write_json_file(
                            "/tmp/mentat/agent.json",
                            &serde_json::json!({ "pid": c.id(), "group": group }),
                        );
                        println!(
                            "mentatd agent started (group={group}, daemon={daemon_addr}); \
                             registration retries until the daemon is reachable"
                        );
                    }
                    Err(e) => {
                        eprintln!("failed to start mentatd agent: {e}");
                        std::process::exit(1);
                    }
                }
            }
        }
        Some(Cmd::InternalAgent { group, daemon }) => {
            agent::run(agent::AgentOpts {
                daemon_addr: daemon,
                group,
            });
        }
        Some(Cmd::Status {
            address,
            group,
            json,
        }) => {
            let addr = resolve_address(address);
            let scope = group.or_else(group_env);
            match cli_request(
                &addr,
                Msg::Status {
                    group: scope.clone(),
                },
            ) {
                Ok((Msg::StatusOk { data }, _)) => {
                    if json {
                        println!("{data}");
                    } else {
                        print!("{}", status::render(&data, scope.is_some()));
                    }
                }
                Ok((other, _)) => {
                    eprintln!("unexpected reply: {other:?}");
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("mentatd: cannot reach daemon at {addr}: {e}");
                    std::process::exit(1);
                }
            }
        }
        Some(Cmd::Stop { address, group }) => {
            let addr = resolve_address(address);
            match cli_request(&addr, Msg::StopAll { group }) {
                Ok(_) => println!("stop sent"),
                Err(e) => {
                    eprintln!("mentatd: cannot reach daemon at {addr}: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}

fn group_env() -> Option<String> {
    std::env::var("MENTAT_GROUP")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("SERVICE_NAME").ok().filter(|s| !s.is_empty()))
}

fn group_from_env() -> String {
    group_env().unwrap_or_else(|| "default".to_string())
}

/// Address precedence: flag, RAY_ADDRESS, head.json, localhost.
fn resolve_address(flag: Option<String>) -> String {
    if let Some(a) = flag.filter(|s| !s.is_empty()) {
        return a;
    }
    if let Ok(a) = std::env::var("RAY_ADDRESS") {
        if !a.is_empty() {
            return a;
        }
    }
    if let Ok(text) = std::fs::read_to_string("/tmp/mentat/head.json") {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(a) = v["address"].as_str() {
                return a.to_string();
            }
        }
    }
    "127.0.0.1:6379".to_string()
}

/// One-shot CLI request: hello, request, response.
fn cli_request(addr: &str, msg: Msg) -> std::io::Result<(Msg, Vec<u8>)> {
    let stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true).ok();
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    write_frame(
        &mut writer,
        &Frame {
            req: 1,
            msg: Msg::Hello {
                client_id: state::random_hex_id(),
                group: group_from_env(),
                session: false,
                kind: "cli".to_string(),
            },
        },
        &[],
    )?;
    let (hello, _) = read_frame(&mut reader)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF at hello"))?;
    if let Msg::Err { error } = hello.msg {
        return Err(std::io::Error::other(error));
    }
    write_frame(&mut writer, &Frame { req: 2, msg }, &[])?;
    let (resp, payload) = read_frame(&mut reader)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF at reply"))?;
    if let Msg::Err { error } = resp.msg {
        return Err(std::io::Error::other(error));
    }
    Ok((resp.msg, payload))
}

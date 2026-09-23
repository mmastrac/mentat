# spec

`HeadDaemon.tla` is a TLA+ model of the head daemon's placement state for
one group: driver sessions, the session reap and its grace, placement
groups, actors, and a daemon restart that adopts the actors still running.

```
scripts/check-spec
```

checks the model with TLC. `scripts/tlc` fetches a pinned `tla2tools.jar`
into `~/.cache/mentat` and needs Java 11 or later. To run TLC by hand:

```
scripts/tlc -config spec/HeadDaemon.cfg spec/HeadDaemon.tla
```

## Properties

| Property | Kind | Statement |
| --- | --- | --- |
| `NoDoubleAlloc` | Invariant | A device has at most one live holder, counting a group and its own actor as one |
| `NoStall` | Liveness | A pending group that its expected devices fit is placed |
| `GraceHoldsActors` | Action | During a driver's reap grace, only that reap kills its actors |
| `SessionKeepsActors` | Action | A driver that holds the session keeps its actors |

## Bug constants

Each `Bug` constant restores a defect that shipped. `check-spec` sets each
one alone and requires the property it broke to fail, so the spec is shown
to find that defect.

| Constant | Defect | Fails |
| --- | --- | --- |
| `BugAccountPgsOnly` | Free devices subtract placement groups alone | `NoDoubleAlloc` |
| `BugNoPlaceOnExit` | An actor's death places nothing | `NoStall` |
| `BugNoOrphanReap` | A new session leaves a gone driver's actors running | `NoStall` |
| `BugKillDuringGrace` | A new session kills actors inside a reap grace | `GraceHoldsActors` |
| `BugReapReconnected` | A deferred reap kills a driver that reopened its session | `SessionKeepsActors` |

## Scope

The model covers one group on the head. It leaves out the mesh, election,
claims, the event stream and byte-level formats. TLC checks every
behaviour up to 3 devices, 3 placement groups, 2 drivers and 1 daemon
restart. A pass means the model holds at that size.

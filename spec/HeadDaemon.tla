----------------------------- MODULE HeadDaemon -----------------------------
(***************************************************************************)
(* The head daemon's placement state for one group: driver sessions, the   *)
(* session reap and its grace, placement groups, actors, and a daemon      *)
(* restart that adopts the actors still running.                           *)
(*                                                                         *)
(* Each Bug constant restores a defect that shipped. scripts/check-spec    *)
(* sets each one alone and requires the property it broke to fail, so the *)
(* spec is shown to find that defect.                                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Clients,        \* driver client ids in the group
    Gpus,           \* the group's devices
    MaxPgs,         \* placement groups created in one behaviour
    MaxRestarts,    \* daemon restarts in one behaviour
    NoClient,       \* the owner of an unused slot
    GraceDeferred,  \* MENTAT_SESSION_REAP_GRACE_MS is above 0

    \* Free devices subtract placement groups alone, so a dying or adopted
    \* actor's devices go to a second rank.
    BugAccountPgsOnly,
    \* An actor's death places nothing, so a group waiting on its devices
    \* sits PENDING until the pending timeout.
    BugNoPlaceOnExit,
    \* A new session leaves a gone driver's actors running. An adopted actor
    \* then holds its devices with no reap to free them.
    BugNoOrphanReap,
    \* A new session kills the actors of a driver inside its reap grace.
    BugKillDuringGrace,
    \* A deferred reap kills a driver that reopened its session.
    BugReapReconnected

VARIABLES
    session,      \* [Clients -> BOOLEAN]: the client holds the driver session
    reapPending,  \* clients whose reap waits out the grace
    pg,           \* [PgIds -> record]: placement group slots
    actor,        \* [PgIds -> record]: the actor placed in each group
    nextPg,       \* the next unused slot
    restarts

vars == <<session, reapPending, pg, actor, nextPg, restarts>>

PgIds == 1..MaxPgs

\* An actor process that holds its devices. A killed one holds them until
\* the agent reports its exit.
Live == {"running", "killing"}

TypeOK ==
    /\ session \in [Clients -> BOOLEAN]
    /\ reapPending \subseteq Clients
    /\ pg \in [PgIds -> [st : {"none", "pending", "created", "removed"},
                         owner : Clients \cup {NoClient},
                         need : 0..Cardinality(Gpus),
                         gpus : SUBSET Gpus]]
    /\ actor \in [PgIds -> [st : {"none", "running", "killing", "dead"},
                            owner : Clients \cup {NoClient},
                            gpus : SUBSET Gpus]]
    /\ nextPg \in 1..(MaxPgs + 1)
    /\ restarts \in 0..MaxRestarts

Init ==
    /\ session = [c \in Clients |-> FALSE]
    /\ reapPending = {}
    /\ pg = [i \in PgIds |-> [st |-> "none", owner |-> NoClient,
                              need |-> 0, gpus |-> {}]]
    /\ actor = [i \in PgIds |-> [st |-> "none", owner |-> NoClient, gpus |-> {}]]
    /\ nextPg = 1
    /\ restarts = 0

(***************************************************************************)
(* State::free_gpus_of and try_place.                                      *)
(***************************************************************************)

Held(p, a) ==
    UNION {p[i].gpus : i \in {j \in PgIds : p[j].st = "created"}}
    \cup IF BugAccountPgsOnly
         THEN {}
         ELSE UNION {a[i].gpus : i \in {j \in PgIds : a[j].st \in Live}}

Free(p, a) == Gpus \ Held(p, a)

\* Each pending group, in slot order, gets devices if enough are free.
RECURSIVE PlaceFrom(_, _, _)
PlaceFrom(i, p, a) ==
    IF i > MaxPgs THEN p
    ELSE IF p[i].st = "pending" /\ Cardinality(Free(p, a)) >= p[i].need
         THEN PlaceFrom(i + 1,
                        [p EXCEPT ![i].st = "created",
                                  ![i].gpus = CHOOSE s \in SUBSET Free(p, a) :
                                                  Cardinality(s) = p[i].need],
                        a)
         ELSE PlaceFrom(i + 1, p, a)

TryPlace(p, a) == PlaceFrom(1, p, a)

(***************************************************************************)
(* Actions.                                                                *)
(***************************************************************************)

\* reap_client_resources: the owner's groups go, its actors are killed, and
\* placement runs.
Reap(c) ==
    LET p == [i \in PgIds |->
                IF pg[i].owner = c /\ pg[i].st \in {"pending", "created"}
                THEN [pg[i] EXCEPT !.st = "removed"] ELSE pg[i]]
        a == [i \in PgIds |->
                IF actor[i].owner = c /\ actor[i].st = "running"
                THEN [actor[i] EXCEPT !.st = "killing"] ELSE actor[i]]
    IN /\ actor' = a
       /\ pg' = TryPlace(p, a)

\* A driver opens the session. The group allows one, so every other session
\* is closed. The daemon kills each running actor whose owner lacks both a
\* session and a pending reap.
Connect(c) ==
    /\ \A d \in Clients : ~session[d]
    /\ session' = [session EXCEPT ![c] = TRUE]
    /\ actor' = [i \in PgIds |->
                   IF /\ ~BugNoOrphanReap
                      /\ actor[i].st = "running"
                      /\ actor[i].owner # c
                      /\ BugKillDuringGrace \/ actor[i].owner \notin reapPending
                   THEN [actor[i] EXCEPT !.st = "killing"]
                   ELSE actor[i]]
    /\ UNCHANGED <<reapPending, pg, nextPg, restarts>>

\* The session closes. With a grace the reap waits. Without one it runs now.
Disconnect(c) ==
    /\ session[c]
    /\ session' = [session EXCEPT ![c] = FALSE]
    /\ IF GraceDeferred
       THEN /\ reapPending' = reapPending \cup {c}
            /\ UNCHANGED <<pg, actor>>
       ELSE /\ Reap(c)
            /\ UNCHANGED reapPending
    /\ UNCHANGED <<nextPg, restarts>>

\* The grace ends. A driver that reopened its session keeps its groups and
\* actors.
ReapFires(c) ==
    /\ c \in reapPending
    /\ reapPending' = reapPending \ {c}
    /\ IF session[c] /\ ~BugReapReconnected
       THEN UNCHANGED <<pg, actor>>
       ELSE Reap(c)
    /\ UNCHANGED <<session, nextPg, restarts>>

PgCreate(c, n) ==
    /\ session[c]
    /\ nextPg <= MaxPgs
    /\ pg' = TryPlace([pg EXCEPT ![nextPg] = [st |-> "pending", owner |-> c,
                                              need |-> n, gpus |-> {}]],
                      actor)
    /\ nextPg' = nextPg + 1
    /\ UNCHANGED <<session, reapPending, actor, restarts>>

\* An actor holds the devices of the group it is placed in.
ActorCreate(i) ==
    /\ pg[i].st = "created"
    /\ actor[i].st = "none"
    /\ session[pg[i].owner]
    /\ actor' = [actor EXCEPT ![i] = [st |-> "running", owner |-> pg[i].owner,
                                      gpus |-> pg[i].gpus]]
    /\ UNCHANGED <<session, reapPending, pg, nextPg, restarts>>

\* mark_actor_dead: the agent reports the process gone, which frees its
\* devices.
Dies(i) ==
    /\ actor' = [actor EXCEPT ![i].st = "dead"]
    /\ pg' = IF BugNoPlaceOnExit THEN pg ELSE TryPlace(pg, actor')
    /\ UNCHANGED <<session, reapPending, nextPg, restarts>>

\* A killed process exits. Fairness guarantees this step.
KilledExits(i) == actor[i].st = "killing" /\ Dies(i)

\* A running process crashes.
Crashes(i) == actor[i].st = "running" /\ Dies(i)

\* The daemon restarts and loses its sessions, reaps and groups. Each agent
\* re-registers with its live processes, and the daemon adopts them.
Restart ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ session' = [c \in Clients |-> FALSE]
    /\ reapPending' = {}
    /\ pg' = [i \in PgIds |->
                IF pg[i].st \in {"pending", "created"}
                THEN [pg[i] EXCEPT !.st = "removed"] ELSE pg[i]]
    /\ UNCHANGED <<actor, nextPg>>

Next ==
    \/ \E c \in Clients : Connect(c) \/ Disconnect(c) \/ ReapFires(c)
    \/ \E c \in Clients, n \in 1..Cardinality(Gpus) : PgCreate(c, n)
    \/ \E i \in PgIds : ActorCreate(i) \/ KilledExits(i) \/ Crashes(i)
    \/ Restart

\* A killed process exits, and a grace ends.
Fairness ==
    /\ \A i \in PgIds : WF_vars(KilledExits(i))
    /\ \A c \in Clients : WF_vars(ReapFires(c))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* A group and the actor placed in it share devices. Every other pair of
\* live holders is disjoint.
NoDoubleAlloc ==
    /\ \A i, j \in PgIds :
           (i # j /\ pg[i].st = "created" /\ pg[j].st = "created")
               => pg[i].gpus \cap pg[j].gpus = {}
    /\ \A i, j \in PgIds :
           (i # j /\ actor[i].st \in Live /\ pg[j].st = "created")
               => actor[i].gpus \cap pg[j].gpus = {}
    /\ \A i, j \in PgIds :
           (i # j /\ actor[i].st \in Live /\ actor[j].st \in Live)
               => actor[i].gpus \cap actor[j].gpus = {}

\* A live actor whose devices come free without anyone's help: its process
\* was killed, or it belongs to a gone driver while another driver holds the
\* session.
Reclaimable(i) ==
    \/ actor[i].st = "killing"
    \/ /\ actor[i].st = "running"
       /\ ~session[actor[i].owner]
       /\ actor[i].owner \notin reapPending
       /\ \E c \in Clients : session[c]

\* Devices a pending group can count on: those held by nothing, or only by a
\* reclaimable actor.
Expected ==
    Gpus \ (UNION {pg[i].gpus : i \in {j \in PgIds : pg[j].st = "created"}}
            \cup UNION {actor[i].gpus :
                          i \in {j \in PgIds : actor[j].st \in Live
                                               /\ ~Reclaimable(j)}})

\* A pending group that the expected devices fit is placed, or stops fitting
\* because another group got them first.
NoStall ==
    \A i \in PgIds :
        (pg[i].st = "pending" /\ Cardinality(Expected) >= pg[i].need)
            ~> (pg[i].st # "pending" \/ Cardinality(Expected) < pg[i].need)

\* The grace keeps a gone driver's actors up. Only its own reap kills them.
GraceHoldsActors ==
    [][\A i \in PgIds :
          (/\ actor[i].st = "running"
           /\ actor'[i].st = "killing"
           /\ actor[i].owner \in reapPending)
              => actor[i].owner \notin reapPending']_vars

\* A driver that holds the session keeps its actors.
SessionKeepsActors ==
    [][\A i \in PgIds :
          (actor[i].st = "running" /\ actor'[i].st = "killing")
              => ~session'[actor[i].owner]]_vars

=============================================================================

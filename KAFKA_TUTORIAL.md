# Kafka — Beginner to Expert Tutorial
### Built on YOUR codebase (`spring_rc`) — Real code, Real examples, Zero fluff

---

## Table of Contents

1. [What Problem Does Kafka Solve?](#1-what-problem-does-kafka-solve)
2. [Core Concepts — The Mental Model](#2-core-concepts--the-mental-model)
3. [Your Kafka Architecture at a Glance](#3-your-kafka-architecture-at-a-glance)
4. [Topic, Partition, and Offset — Deep Dive](#4-topic-partition-and-offset--deep-dive)
5. [KafkaConfig.java — Every Line Explained](#5-kafkaconfigjava--every-line-explained)
6. [Producer Deep Dive — RoleAssignmentProducer.java](#6-producer-deep-dive--roleassignmentproducerjava)
7. [Consumer Deep Dive — RoleAssignmentConsumer.java](#7-consumer-deep-dive--roleassignmentconsumerjava)
8. [The Full Flow — What Happens Step by Step](#8-the-full-flow--what-happens-step-by-step)
9. [What Happens If You Don't Do This?](#9-what-happens-if-you-dont-do-this)
10. [Error Handling — Retry vs Non-Retry](#10-error-handling--retry-vs-non-retry)
11. [Serialization & Deserialization](#11-serialization--deserialization)
12. [Idempotency & Duplicate Safety](#12-idempotency--duplicate-safety)
13. [Consumer Groups](#13-consumer-groups)
14. [Offset Management — The Most Critical Concept](#14-offset-management--the-most-critical-concept)
15. [KRaft Mode — No More ZooKeeper](#15-kraft-mode--no-more-zookeeper)
16. [Docker Compose — Kafka Setup Explained](#16-docker-compose--kafka-setup-explained)
17. [Best Practices — Beginner to Expert](#17-best-practices--beginner-to-expert)
18. [Common Mistakes & What Goes Wrong](#18-common-mistakes--what-goes-wrong)
19. [Glossary](#19-glossary)

---

## 1. What Problem Does Kafka Solve?

### The Old Way (Synchronous — What you'd do without Kafka)

Imagine a user hits your API: `PUT /api/v1/employees/5/assign-role`

Without Kafka, this is what happens:

```
User Request
    │
    ▼
EmployeeController  (HTTP thread is BLOCKED here)
    │
    ▼
EmployeeService.assignRole()
    │
    ▼
DB query: findById(5)           ← what if DB is slow? (2s wait)
    │
    ▼
DB query: assignRole(5, 2)      ← what if DB is down? (timeout)
    │
    ▼
HTTP Response returned          ← user waited 2+ seconds
```

**Problems with this approach:**
- If the DB is down → the API call fails immediately → **data is LOST**
- If DB is slow → the HTTP thread is **blocked** waiting (thread starvation)
- If 1000 users call at the same time → 1000 threads all stuck waiting on DB
- **Tight coupling**: your API speed = your DB speed

---

### The Kafka Way (Asynchronous — What YOUR code does)

```
User Request
    │
    ▼
EmployeeController  (HTTP thread is FREE in milliseconds)
    │
    ▼
EmployeeService.assignRole()
    │
    ├── Best-effort DB check (optional fast-fail)
    │
    ▼
RoleAssignmentProducer.publish()   ← just puts a message in Kafka
    │
    ▼
HTTP 202 Accepted returned immediately ← user gets response in <50ms
    
    (Meanwhile, in the background...)
    
Kafka Topic: employee.role-assignment
    │
    ▼
RoleAssignmentConsumer.onAssignRole()   ← separate thread, retries on failure
    │
    ▼
DB query: findById(), assignRole()      ← if DB is down, Kafka retries automatically
```

**Benefits:**
- API never fails because the DB is down → **zero data loss**
- HTTP thread free in milliseconds → **high throughput**
- DB can be down for hours → Kafka holds the event → retries until DB comes back
- **Loose coupling**: API speed is decoupled from DB speed

---

## 2. Core Concepts — The Mental Model

Think of Kafka like a **Post Office**:

```
Producer  =  The person who writes and sends a letter (your Spring app)
Topic     =  The mailbox category (e.g., "employee.role-assignment")
Partition =  Individual slots inside the mailbox
Offset    =  The sequential number on each letter (0, 1, 2, 3, ...)
Consumer  =  The person who reads the letters (your Spring app, different class)
Broker    =  The post office building (Kafka server)
```

### Visual:

```
PRODUCER                    KAFKA BROKER                    CONSUMER
   │                                                            │
   │    publish(employeeId=5, roleId=2)                        │
   │ ─────────────────────────────────────► Topic:             │
   │                                    "employee.role-        │
   │                                     assignment"           │
   │                                                           │
   │                                    Partition 0:           │
   │                                    [msg0][msg1][msg2] ───►│ reads msg0
   │                                                           │ reads msg1
   │                                    Partition 1:           │
   │                                    [msg3][msg4]      ───►│ reads msg3
   │                                                           │
   │                                    Partition 2:           │
   │                                    [msg5]           ───►│ reads msg5
```

---

## 3. Your Kafka Architecture at a Glance

Here's exactly how YOUR spring_rc project uses Kafka:

```
Browser / API Client
        │
        │  PUT /api/v1/employees/5/assign-role
        │  Body: { "employeeId": 5, "roleId": 2 }
        ▼
EmployeeController.assignRole()
        │
        ▼
EmployeeService.assignRole(employeeId=5, roleId=2)
        │
        ├──► [Optional] DB check: findById(5) — fast-fail if employee not found
        │
        ▼
RoleAssignmentProducer.publish(5, 2)
        │
        │  Creates AssignRoleEvent {
        │    eventId: "uuid-abc-123",
        │    employeeId: 5,
        │    roleId: 2,
        │    requestedAt: "2024-01-15T10:30:00Z"
        │  }
        │
        │  Sends to topic: "employee.role-assignment"
        │  Key: "5"  (employeeId as String)
        │
        ▼
KafkaTemplate.send()
        │
        ▼
KAFKA BROKER (kafka:9092)
  Topic: employee.role-assignment
  Partition chosen by: hash("5") % 3 = Partition 2 (same key = same partition always)
        │
        │  (stored durably on disk)
        │
        ▼
RoleAssignmentConsumer.onAssignRole(event)  ← separate thread
        │
        ├──► DB: findById(5) → check if role_id is null
        ├──► DB: assignRole(5, 2)
        │
        ▼
     SUCCESS → offset committed → message consumed ✓
     FAILURE → retry after 5 seconds (indefinitely for transient errors)
               → NonRetryableAssignmentException → skip retry (permanent errors)
```

---

## 4. Topic, Partition, and Offset — Deep Dive

### Topic

A **topic** is a named channel/category for messages.

In your code:
```java
// KafkaConfig.java
public static final String ROLE_ASSIGNMENT_TOPIC = "employee.role-assignment";
```

Naming convention: use dots or hyphens. Your topic `employee.role-assignment` follows the `domain.action` pattern — perfect.

---

### Partition

A topic is split into **partitions**. Each partition is an ordered, immutable log.

In your code:
```java
@Bean
public NewTopic roleAssignmentTopic() {
    return TopicBuilder.name(ROLE_ASSIGNMENT_TOPIC)
            .partitions(3)   // ← 3 partitions
            .replicas(2)     // ← 2 replicas (copies) of each partition
            .build();
}
```

**Why 3 partitions?**

```
Without partitions (1 partition):
  Consumer 1 reads: msg1, msg2, msg3, msg4, msg5...  (sequential, slow)

With 3 partitions (3 consumers can run in parallel):
  Consumer 1 reads Partition 0: msg1, msg4, msg7...
  Consumer 2 reads Partition 1: msg2, msg5, msg8...
  Consumer 3 reads Partition 2: msg3, msg6, msg9...
```

**3x throughput** with 3 partitions and 3 consumers!

---

### How Your Key Determines the Partition

In your Producer:
```java
kafkaTemplate.send(
    KafkaConfig.ROLE_ASSIGNMENT_TOPIC,
    String.valueOf(employeeId),   // ← THIS IS THE KEY
    event
);
```

Kafka uses: `hash(key) % numPartitions` to decide which partition.

```
employeeId=5  → hash("5") % 3 = 2  → always goes to Partition 2
employeeId=5  → hash("5") % 3 = 2  → always goes to Partition 2  (same!)
employeeId=5  → hash("5") % 3 = 2  → always goes to Partition 2  (always!)
```

**Why this matters:**
All events for employee 5 always land on the same partition → **processed in order**.
If employee 5 gets two role-assignment events, they'll be processed sequentially, not in parallel.

> ⚠️ **What if you don't set a key?**
> Events go to random partitions in round-robin. Two events for the same employee
> could be processed by different consumers simultaneously → **race condition on your DB!**

---

### Offset

Every message in a partition has a sequential **offset** number:

```
Partition 2:
┌─────────────────────────────────────────────────────┐
│ offset=0  │ offset=1  │ offset=2  │ offset=3  │ ... │
│ emp5→r1   │ emp5→r3   │ emp5→r7   │ emp11→r2  │ ... │
└─────────────────────────────────────────────────────┘
              ▲
              │
     Consumer has read up to here (committed offset=1)
     Next read will start at offset=2
```

The offset is how Kafka tracks "what has this consumer group already processed?"

---

## 5. KafkaConfig.java — Every Line Explained

Let's walk through every config property in your file and why it matters.

### 5.1 Bootstrap Servers

```java
private static String bootstrapServers() {
    String val = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    return (val != null && !val.isBlank()) ? val : "kafka:9092";
}
```

**What it does:** Tells clients where Kafka is. This is just the initial connection point — once connected, Kafka tells the client about all the other brokers.

**Environment-aware:** In Docker it's `kafka:9092` (service name), locally it'd be `localhost:9092`.

> ⚠️ **What if you hardcode `localhost:9092`?**
> Works locally. Breaks in Docker/production because containers don't resolve `localhost` as the Kafka container. Always use env vars.

---

### 5.2 KafkaAdmin — Auto Topic Creation

```java
@Bean
public KafkaAdmin kafkaAdmin() {
    Map<String, Object> config = new HashMap<>();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    return new KafkaAdmin(config);
}
```

**What it does:** Lets Spring auto-create topics via `@Bean NewTopic`.

> ⚠️ **What if you skip KafkaAdmin?**
> Your `NewTopic` bean does nothing. If the topic doesn't exist in Kafka, producers fail with `UNKNOWN_TOPIC_OR_PARTITION` error.

---

### 5.3 Producer Config

```java
config.put(ProducerConfig.ACKS_CONFIG, "all");
```

**`acks` — Acknowledgement level:**

| Value | Meaning | Risk |
|-------|---------|------|
| `0` | Don't wait for any ack. Fire and forget. | High data loss |
| `1` | Wait for leader broker to write. | Moderate loss (if leader crashes before replication) |
| `all` | Wait for ALL in-sync replicas to confirm write | Zero data loss ✓ |

Your code uses `"all"` → **strongest guarantee**. Slightly slower but never loses a message.

---

```java
config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```

**Idempotence:** Makes the producer retry-safe.

Without idempotence:
```
Producer sends msg1 → Broker writes it → Broker crashes before sending ACK
Producer retries   → Broker writes msg1 AGAIN → DUPLICATE!
```

With idempotence:
```
Producer sends msg1 (with sequence number 42)
Broker writes it → crashes before ACK
Producer retries with same sequence 42
Broker sees "already have sequence 42" → ignores duplicate → only one msg ✓
```

---

```java
config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
```

**Why `Integer.MAX_VALUE`?**
Combined with idempotence, retrying indefinitely is safe (no duplicates). Without idempotence, unlimited retries = duplicate risk.

---

### 5.4 Consumer Config

```java
config.put(ConsumerConfig.GROUP_ID_CONFIG, "employee-role-assignment-group");
```

**Consumer Group:** All consumers with the same `group_id` share the work.

```
group: "employee-role-assignment-group"
  ├── Consumer Instance 1 → reads Partition 0
  ├── Consumer Instance 2 → reads Partition 1
  └── Consumer Instance 3 → reads Partition 2
```

If you scale to 2 app instances, each one has a consumer. Kafka distributes partitions between them automatically (**rebalancing**).

---

```java
config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
```

**What it does:** When a new consumer group starts (no saved offset yet), where do we start reading?

| Value | Meaning |
|-------|---------|
| `earliest` | Start from the very beginning of the topic |
| `latest` | Only read messages published AFTER the consumer starts |

Your code uses `earliest` → if the consumer was down for 2 hours, it will **catch up on all missed messages** when it comes back. Perfect for role-assignment — you don't want to miss any!

> ⚠️ **What if you use `latest`?**
> Consumer was down for 2 hours. 500 role-assignment events came in. Consumer restarts.
> It skips all 500 events. **500 role assignments are silently lost forever.**

---

```java
config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
```

**The most critical consumer setting.**

With `true` (auto-commit):
```
Consumer reads msg at offset=5
Kafka auto-commits offset=6 (every 5 seconds by default)
Consumer crashes while processing msg at offset=5
Consumer restarts → starts from offset=6 → msg=5 is SKIPPED FOREVER!
```

With `false` (manual commit — what your code does):
```
Consumer reads msg at offset=5
Consumer processes it (DB write succeeds)
Spring Kafka commits offset=6 automatically AFTER successful processing
Consumer crashes while processing → restarts at offset=5 → retries it ✓
```

> ⚠️ **This is the single biggest source of data loss in Kafka applications.**
> Always use `ENABLE_AUTO_COMMIT_CONFIG = false` for critical workflows.

---

```java
config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
```

**Why 1?**

Kafka's consumer works in batches. If `MAX_POLL_RECORDS = 500`:
- Consumer fetches 500 messages
- Processes message 1 successfully
- Fails on message 2
- Error handler retries — but which message does it retry? Message 2.
- But the DB might now get confused about state from message 1 + retrying message 2

By setting it to 1:
- Each `poll()` returns exactly 1 message
- Clear, predictable retry behaviour
- The error handler retries that single message with no ambiguity

Trade-off: Lower throughput. But for role-assignment, correctness > speed.

---

### 5.5 The Error Handler

```java
DefaultErrorHandler errorHandler =
    new DefaultErrorHandler(new FixedBackOff(RETRY_INTERVAL_MS, FixedBackOff.UNLIMITED_ATTEMPTS));
```

**`FixedBackOff(5000, UNLIMITED_ATTEMPTS)`:**
- Wait 5 seconds between retries
- Retry forever until it succeeds

```
Message fails at 10:00:00 → retry at 10:00:05 → fails
                           → retry at 10:00:10 → fails
                           → retry at 10:00:15 → DB comes back → SUCCESS ✓
```

**`UNLIMITED_ATTEMPTS`** = if your DB goes down at 2am and comes back at 6am, Kafka was retrying every 5 seconds. All events are still processed. **Zero data loss.**

---

```java
errorHandler.addNotRetryableExceptions(NonRetryableAssignmentException.class);
```

**Why this class?** See Section 10.

---

## 6. Producer Deep Dive — RoleAssignmentProducer.java

```java
public void publish(int employeeId, int roleId) {
    AssignRoleEvent event = new AssignRoleEvent(employeeId, roleId);
    
    kafkaTemplate.send(
        KafkaConfig.ROLE_ASSIGNMENT_TOPIC,   // topic name
        String.valueOf(employeeId),           // partition key
        event                                 // payload (serialized to JSON)
    )
    .whenComplete((result, ex) -> {
        if (ex != null) {
            log.error("Failed to publish...", ex);
        } else {
            log.info("Published role assignment event...");
        }
    });
}
```

### `AssignRoleEvent` — Why these fields?

```java
public record AssignRoleEvent(
    String eventId,       // UUID — unique event ID for deduplication & tracing
    int employeeId,       // which employee
    int roleId,           // which role
    Instant requestedAt  // when was this requested (audit trail)
) {}
```

**`eventId` (UUID):** Critical for debugging. If the same event appears twice (duplicate), you can detect it. In logs, search by `eventId` to trace the full lifecycle of one request.

**`requestedAt` (Instant):** Audit trail. "When did the user actually request this?" vs "When did the DB write happen?" — these can be different if the consumer was delayed.

### `whenComplete` — Non-blocking callback

`kafkaTemplate.send()` returns a `CompletableFuture`. Your code uses `.whenComplete()` — this is a **callback**. The calling thread is NOT blocked. 

```
send() called → returns immediately
    (Kafka internal thread publishes the message)
    (callback fires when done — success or failure)
    
Main thread already returned HTTP 202 to user ✓
```

> ⚠️ **What if you call `.get()` instead of `.whenComplete()`?**
> ```java
> kafkaTemplate.send(...).get();  // BLOCKS the thread!
> ```
> Your HTTP thread now blocks until Kafka confirms the publish. Under high load, all threads block → your server is unresponsive. Never do this.

---

## 7. Consumer Deep Dive — RoleAssignmentConsumer.java

```java
@KafkaListener(
    topics = KafkaConfig.ROLE_ASSIGNMENT_TOPIC,
    groupId = "employee-role-assignment-group",
    containerFactory = "kafkaListenerContainerFactory"
)
public void onAssignRole(AssignRoleEvent event) throws Exception {
```

**`@KafkaListener`:** Spring creates a background thread that continuously polls Kafka and calls this method for each message.

**`containerFactory = "kafkaListenerContainerFactory"`:** Tells Spring which factory bean to use (your custom one with the error handler, not the default).

> ⚠️ **What if you omit `containerFactory`?**
> Spring uses the default factory — no custom error handler, no retries, no `MAX_POLL_RECORDS=1`. Errors are silently dropped. Messages are lost.

---

### The Validation Logic

```java
Map<String, Object> employee = employeeRepository.findById(event.employeeId());
if (employee.get("role_id") != null) {
    throw ExceptionUtil.badRequest(
        "Employee " + event.employeeId() + " already has a role assigned");
}
employeeRepository.assignRole(event.employeeId(), event.roleId());
```

**Why validate AGAIN in the consumer?** (It's also validated in `EmployeeService`)

Because Kafka is async — between the time the API returns 202 and the consumer processes the event:
- Someone else could have assigned a role via a different API call
- The consumer might be processing a retry of a duplicate event

The consumer is the **source of truth** for what actually happens to the DB. Always validate there.

This is called **consumer-side idempotency**: "Even if I get this message twice, I won't corrupt the data."

---

## 8. The Full Flow — What Happens Step by Step

Let's trace a real request end to end:

```
Step 1: API Request arrives
─────────────────────────────
PUT /api/v1/employees/5/assign-role
Body: { "employeeId": 5, "roleId": 2 }

Step 2: Controller
─────────────────────────────
EmployeeController.assignRole() is called
→ delegates to EmployeeService.assignRole(5, 2)

Step 3: Service — Best-effort pre-check
─────────────────────────────
EmployeeService.assignRole(5, 2)
→ tries DB: findById(5)
  ✓ Employee found, role_id is null → OK to proceed
  ✗ Employee not found → throw 404 immediately (no Kafka)
  ✗ Role already assigned → throw 400 immediately (no Kafka)
  ✗ DB down → swallow error, still queue the event (don't lose the request!)

Step 4: Produce to Kafka
─────────────────────────────
RoleAssignmentProducer.publish(5, 2)

Creates event:
{
  "eventId": "f3a1b2c3-...",
  "employeeId": 5,
  "roleId": 2,
  "requestedAt": "2024-01-15T10:30:00Z"
}

Sends to topic "employee.role-assignment"
Key: "5" → hash("5") % 3 = Partition 2

Step 5: API responds
─────────────────────────────
HTTP 202 Accepted
Body: "Role assignment request queued for processing"

(User gets this in ~20ms regardless of DB state)

Step 6: Kafka stores the message
─────────────────────────────
Partition 2, Offset 47:
{ eventId: "f3a1b2c3", employeeId: 5, roleId: 2, requestedAt: "..." }

Step 7: Consumer picks it up
─────────────────────────────
RoleAssignmentConsumer.onAssignRole(event)
→ findById(5) → employee exists, role_id is null ✓
→ assignRole(5, 2) → DB write succeeds ✓
→ method returns normally

Step 8: Offset committed
─────────────────────────────
Spring Kafka sees success → commits offset=48
(Kafka knows this consumer group has processed up to offset 47)
Message is "done" ✓
```

### Failure Scenario — DB Down During Consumer

```
Step 7 (FAILURE):
─────────────────────────────
RoleAssignmentConsumer.onAssignRole(event)
→ findById(5) → DB connection refused → AppException(500) thrown

Error Handler kicks in:
→ Not a NonRetryableAssignmentException
→ Wait 5 seconds...
→ Retry onAssignRole(same event)
→ DB still down → exception again
→ Wait 5 seconds...
→ Retry... (repeat until DB comes back)

(2 hours later, DB comes back)
→ Retry onAssignRole(same event)
→ findById(5) → employee exists, role_id is null ✓
→ assignRole(5, 2) → SUCCESS ✓
→ offset committed ✓

Result: Zero data loss. The role was assigned, just 2 hours later.
```

---

## 9. What Happens If You Don't Do This?

This section answers: **"What breaks if I remove/change X?"**

---

### 9.1 No Kafka — Synchronous DB write in controller

```java
// BAD — what your code would look like without Kafka
@PutMapping("/{employeeId}/assign-role")
public ResponseEntity<?> assignRole(...) {
    employee = db.findById(employeeId);   // DB call 1 — if DB down: 500 error
    db.assignRole(employeeId, roleId);    // DB call 2 — if DB down: 500 error
    return ResponseEntity.ok("done");
}
```

**Result if DB is down:** User gets `500 Internal Server Error`. The request is gone. Lost.  
**Result under load:** 1000 concurrent requests → 1000 threads blocked on DB → server OOM.

---

### 9.2 No `acks=all`

```java
config.put(ProducerConfig.ACKS_CONFIG, "1");  // only leader must confirm
```

**Scenario:**
1. Message sent to leader broker
2. Leader writes to disk, sends ACK to producer
3. Leader crashes before replicating to follower
4. Follower becomes new leader — message is **gone**
5. Producer thinks it was delivered ✓ — it wasn't ✗

Use `acks=all` when you can't afford data loss.

---

### 9.3 No `enable.idempotence=true`

```java
config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);  // dangerous!
```

**Scenario:**
1. Producer sends message
2. Broker writes it, gets interrupted before sending ACK
3. Producer retries (due to `RETRIES_CONFIG = Integer.MAX_VALUE`)
4. Broker writes the same message AGAIN
5. Consumer gets it twice → employee gets assigned same role twice → DB error or wrong state

With idempotence: step 4 is skipped. Broker recognizes the sequence number. Single write. ✓

---

### 9.4 No `ENABLE_AUTO_COMMIT_CONFIG = false`

```java
config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);  // dangerous!
```

**Default auto-commit interval: 5 seconds.**

**Scenario:**
1. Consumer reads message at offset=10 at time T
2. At T+3s, Kafka auto-commits offset=11 (says "we processed up to 10")
3. At T+4s, consumer crashes while processing offset=10
4. Consumer restarts → reads from offset=11 (because that was committed)
5. Message at offset=10 is **NEVER PROCESSED**

Employee's role assignment request silently vanished. No error. No log. Gone.

---

### 9.5 No partition key (no `String.valueOf(employeeId)` as key)

```java
kafkaTemplate.send(ROLE_ASSIGNMENT_TOPIC, event);  // no key!
```

**Scenario:**
- Employee 5 makes two role-assignment requests 1 second apart
- Request A → goes to Partition 0 (Consumer A picks it up)
- Request B → goes to Partition 2 (Consumer B picks it up)
- Consumer B processes Request B first (it was faster)
- DB: employee 5 gets roleId=2
- Consumer A processes Request A
- DB: employee 5 already has role → **409 Conflict** or overwritten!

With a key, both go to the same partition, same consumer, processed in order. ✓

---

### 9.6 No `NonRetryableAssignmentException` — retrying permanent failures

```java
// No not-retryable exceptions added to error handler
// What happens when employee doesn't exist?
```

**Scenario:**
- Event says: assign role 2 to employee 999
- Employee 999 doesn't exist (was deleted)
- Consumer throws `404 NOT_FOUND`
- Error handler retries after 5 seconds
- Retries again... and again... and again... **FOREVER**
- This partition is **STUCK**. No new messages for any employee on this partition are processed.
- Your system grinds to a halt.

With `NonRetryableAssignmentException`: permanent failures are caught, logged, and skipped. The partition moves on.

---

### 9.7 `AUTO_OFFSET_RESET_CONFIG = "latest"` instead of `"earliest"`

**Scenario:**
- Your consumer service crashes at 2:00 PM
- 300 role-assignment events come in between 2:00 PM and 4:00 PM
- Consumer comes back online at 4:00 PM
- With `latest`: starts reading from 4:00 PM onwards → 300 events **silently skipped**
- With `earliest`: catches up from where it left off → all 300 events processed ✓

---

## 10. Error Handling — Retry vs Non-Retry

This is the most nuanced part of your codebase. Read carefully.

### The Problem

`AppException` is used for TWO different kinds of failures:
1. **Business failures** (permanent): "Employee not found (404)", "Already has role (409)"
2. **Technical failures** (transient): "DB connection refused (500)"

Both are `AppException`, but they should be handled **completely differently**:
- Business failures → **DON'T retry** (retrying 1000 times won't make the employee exist)
- Technical failures → **DO retry** (retrying works once the DB comes back)

### Your Solution

```java
// In RoleAssignmentConsumer.java
private static final Set<HttpStatus> PERMANENT_STATUSES =
    Set.of(HttpStatus.BAD_REQUEST, HttpStatus.NOT_FOUND, HttpStatus.CONFLICT);

catch (AppException ae) {
    if (PERMANENT_STATUSES.contains(ae.getStatus())) {
        // This is a business failure — will never succeed → don't retry
        throw new NonRetryableAssignmentException(ae.getMessage(), ae);
    }
    // This is a 500 (technical failure, e.g. DB down) → must retry
    throw ae;
}
```

```java
// In KafkaConfig.java
errorHandler.addNotRetryableExceptions(NonRetryableAssignmentException.class);
```

**Flow:**

```
onAssignRole() throws AppException(404)
    → caught → PERMANENT_STATUSES.contains(NOT_FOUND) = true
    → wrapped in NonRetryableAssignmentException
    → thrown

Error handler sees NonRetryableAssignmentException
    → "this is in my not-retryable list"
    → logs it
    → commits the offset (moves on)
    → this event is DONE (not processed successfully, but not retried)
```

```
onAssignRole() throws AppException(500)
    → caught → PERMANENT_STATUSES.contains(INTERNAL_SERVER_ERROR) = false
    → re-thrown as AppException(500)

Error handler sees AppException (not in not-retryable list)
    → "retry this!"
    → waits 5 seconds
    → calls onAssignRole() again with same event
```

### Retry Decision Tree

```
Exception thrown from onAssignRole()
            │
            ▼
    Is it NonRetryableAssignmentException?
    ├── YES → Skip. Log error. Move to next message.
    └── NO  → Is it in PERMANENT_STATUSES (404, 400, 409)?
              ├── YES → Already wrapped above, won't reach here
              └── NO (500, network error, etc.) → RETRY after 5 seconds
```

---

## 11. Serialization & Deserialization

### Why do you need serializers?

Kafka transfers bytes. Your `AssignRoleEvent` is a Java object. You need to:
- **Serialize**: Java object → JSON bytes (producer side)
- **Deserialize**: JSON bytes → Java object (consumer side)

### Producer side

```java
JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper());
serializer.setAddTypeInfo(false);
```

`setAddTypeInfo(false)` → Don't add a `__TypeId__` header to the message.

**Why?** If you add type info, the consumer MUST have the exact same class in the exact same package. Fragile. By disabling it, the consumer just deserializes whatever JSON it gets into the target class.

### Consumer side

```java
JsonDeserializer<AssignRoleEvent> deserializer =
    new JsonDeserializer<>(AssignRoleEvent.class, kafkaObjectMapper());
deserializer.addTrustedPackages("com.hari.dto");
deserializer.ignoreTypeHeaders();
```

**`addTrustedPackages("com.hari.dto")`:** Spring's `JsonDeserializer` by default rejects deserializing into classes not explicitly trusted (security measure). You whitelist your DTO package.

**`ignoreTypeHeaders()`:** Even if a producer sends a `__TypeId__` header, ignore it. Always deserialize to `AssignRoleEvent`. Consistent with producer's `setAddTypeInfo(false)`.

### The ObjectMapper

```java
private static ObjectMapper kafkaObjectMapper() {
    return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
}
```

**`JavaTimeModule`:** Needed to serialize/deserialize `java.time.Instant`.  
Without it: `requestedAt` field fails to serialize → `InvalidDefinitionException`.

**`WRITE_DATES_AS_TIMESTAMPS = false`:** Serializes `Instant` as ISO-8601 string (`"2024-01-15T10:30:00Z"`) instead of epoch milliseconds (`1705312200000`). Human-readable.

> ⚠️ **What if you use the default Spring-managed ObjectMapper for Kafka?**
> Spring Boot's main `ObjectMapper` might have different modules registered (or not).
> A schema change could silently affect Kafka message format. Always create a dedicated
> ObjectMapper for Kafka (as your code does).

---

## 12. Idempotency & Duplicate Safety

### Producer Idempotency (already covered)

Enabled via `ENABLE_IDEMPOTENCE_CONFIG = true`. Prevents broker-level duplicates.

### Consumer Idempotency (your code)

Your consumer re-checks before writing:

```java
if (employee.get("role_id") != null) {
    throw ExceptionUtil.badRequest("Employee already has a role");
}
employeeRepository.assignRole(event.employeeId(), event.roleId());
```

Even if the same message is delivered twice (edge case with rebalancing):
1. First delivery: role_id is null → assign role → success → offset committed
2. Second delivery (duplicate): role_id is NOT null → throw 400 → `NonRetryableAssignmentException` → skip

**Result: Employee gets the role exactly once.** That's idempotency.

### The Exactly-Once Illusion

Kafka gives you "at-least-once" delivery by default (with manual commit). Messages might be delivered more than once during rebalances or retries. Your consumer-side check (`role_id != null`) is what makes it effectively "exactly-once" from a data perspective.

---

## 13. Consumer Groups

```java
config.put(ConsumerConfig.GROUP_ID_CONFIG, "employee-role-assignment-group");
```

### What is a Consumer Group?

It's a team of consumers that collectively process a topic.

```
Topic: employee.role-assignment
Partition 0 ──► Consumer A  │
Partition 1 ──► Consumer B  │  All in group: "employee-role-assignment-group"
Partition 2 ──► Consumer C  │
```

Each partition is assigned to **exactly one** consumer in the group at a time.

### Scaling

If you run `--scale app=3` in Docker, you get 3 instances of your Spring app:
- Instance 1 → handles Partition 0
- Instance 2 → handles Partition 1
- Instance 3 → handles Partition 2

If one instance dies, Kafka **rebalances**: the remaining two instances each pick up an extra partition.

### What if you have more consumers than partitions?

```
3 partitions, 4 consumers:
Partition 0 ──► Consumer A
Partition 1 ──► Consumer B
Partition 2 ──► Consumer C
             ──  Consumer D (IDLE — no partition to read)
```

Consumer D sits doing nothing. **Partitions are the unit of parallelism.**
This is why you chose 3 partitions — you can scale up to 3 parallel consumers.

---

## 14. Offset Management — The Most Critical Concept

### What is an offset commit?

It's how Kafka tracks "what has this consumer group already processed?"

Your offsets are stored in a special internal Kafka topic: `__consumer_offsets`.

```
Group: "employee-role-assignment-group"
  Partition 0: committed offset = 42  (next read starts at 43)
  Partition 1: committed offset = 17  (next read starts at 18)
  Partition 2: committed offset = 91  (next read starts at 92)
```

### Spring Kafka's AckMode

With `ENABLE_AUTO_COMMIT = false`, Spring Kafka takes control. The default `AckMode` with `ConcurrentKafkaListenerContainerFactory` is **`BATCH`** — offset is committed after each batch (each `poll()`) returns normally.

Since your `MAX_POLL_RECORDS = 1`, each batch = 1 message. So the offset is committed after each message processes successfully.

```
poll() → returns [msg at offset=47]
onAssignRole(msg) → success
Spring commits offset=48 ✓
poll() → returns [msg at offset=48]
...
```

If `onAssignRole(msg)` throws and the error handler retries:
```
poll() → returns [msg at offset=47]
onAssignRole(msg) → throws exception
Error handler retries onAssignRole(msg) ← same msg, NO new poll()
→ throws again
→ retries again
(no offset commit until success)
```

---

## 15. KRaft Mode — No More ZooKeeper

Your Docker Compose runs Kafka in **KRaft mode** (Kafka Raft). This is the modern way.

```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

**Old way (Kafka < 2.8):**
```
ZooKeeper cluster (separate!) ← stores metadata, leader elections
    +
Kafka broker cluster
```

**KRaft way (Kafka 2.8+, default in 3.x):**
```
Kafka broker (also acts as controller, manages metadata internally)
    (ZooKeeper is GONE)
```

**For you:** One less service to run. Simpler setup. Better performance. Your `docker-compose.yml` doesn't have a ZooKeeper container because of this.

```yaml
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
```

Port 9092: Regular Kafka traffic (producers, consumers)  
Port 9093: Internal controller traffic (cluster metadata, leader elections)

---

## 16. Docker Compose — Kafka Setup Explained

```yaml
kafka:
  image: apache/kafka:3.8.0
  environment:
    KAFKA_NODE_ID: 1
    # This broker is BOTH a data broker AND the controller (leader election)
    KAFKA_PROCESS_ROLES: broker,controller
    
    # Listen on 2 ports:
    # - PLAINTEXT:9092 for producers/consumers
    # - CONTROLLER:9093 for internal controller communication
    KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
    
    # Advertise kafka:9092 to clients (Docker network resolves "kafka" to this container)
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    
    # Map listener names to security protocols
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
    
    # Only 1 replica needed (single-broker setup for development)
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    
    # Unique cluster ID (base64 encoded UUID) — required for KRaft
    CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
```

### Why `replicas(2)` in code but `REPLICATION_FACTOR: 1` in Docker?

```java
// KafkaConfig.java
return TopicBuilder.name(ROLE_ASSIGNMENT_TOPIC)
        .partitions(3)
        .replicas(2)   // ← wants 2 replicas
        .build();
```

```yaml
# docker-compose.yml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1  # ← only 1 broker exists!
```

You only have **1 Kafka broker** in development. You can't replicate to 2 brokers when only 1 exists.

Kafka **silently uses min(requested_replicas, available_brokers)** for topic creation. So your topic gets 1 replica in dev, but when deployed to a real cluster with 3 brokers, it'll get 2 replicas.

> ✅ **Best practice:** Set `replicas(2)` or `replicas(3)` in code (for production), but ensure your Docker dev setup has `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` to avoid startup errors.

---

## 17. Best Practices — Beginner to Expert

### Beginner Level

| Practice | Why |
|---------|-----|
| Always use `ENABLE_AUTO_COMMIT = false` | Prevents silent data loss |
| Always set `AUTO_OFFSET_RESET = earliest` for critical data | Catch up on missed messages |
| Use a meaningful topic name like `domain.action` | `employee.role-assignment` is clear |
| Always log `eventId` in every log statement | Makes distributed tracing possible |

### Intermediate Level

| Practice | Why |
|---------|-----|
| Key messages by entity ID | Ordering per entity, prevents race conditions |
| Separate retryable vs non-retryable exceptions | Avoid infinite retry loops |
| Use `acks=all` + `enable.idempotence=true` | Strongest delivery guarantee |
| Set `MAX_POLL_RECORDS=1` for complex processing | Predictable retry behaviour |
| Include `requestedAt` in events | Audit trail, detect stale events |
| Use a dedicated ObjectMapper for Kafka | Isolate serialization config |

### Expert Level

| Practice | Why |
|---------|-----|
| Design consumers to be idempotent | Handle duplicates gracefully (rebalances, retries) |
| Never put business logic in producers | Producers just publish — consumers do the work |
| Use Dead Letter Topics (DLT) for max retries | Don't lose permanently-failed events |
| Monitor consumer lag | `consumer_group_lag` metric = how far behind you are |
| Schema registry (Avro/Protobuf) for large teams | Prevents breaking changes in message format |
| Don't share consumer groups between unrelated services | Accidental partition takeover |
| Compact topics for state (not event) data | Keep only latest value per key |

### Your Code Does These Right ✓

- ✅ Entity-keyed messages (`String.valueOf(employeeId)`)
- ✅ `acks=all` + idempotence
- ✅ Manual offset commit
- ✅ Retry/non-retry separation
- ✅ Consumer-side re-validation (idempotent consumer)
- ✅ Dedicated Kafka ObjectMapper
- ✅ `earliest` offset reset
- ✅ `eventId` (UUID) in every event
- ✅ `requestedAt` for audit trail

---

## 18. Common Mistakes & What Goes Wrong

### Mistake 1: Publishing the event AFTER the DB write

```java
// BAD
public void assignRole(int employeeId, int roleId) {
    db.assignRole(employeeId, roleId);  // DB write first
    kafka.publish(event);               // then publish
}
```

**Problem:** DB write succeeds. Kafka publish fails. You have data in DB but no event. Consumer never fires. Inconsistency.

**Your code does the opposite (correct):** Publish to Kafka first, consumer writes to DB. If DB fails, Kafka retries. ✓

---

### Mistake 2: Consuming from the wrong group ID

```java
// Consumer 1
@KafkaListener(groupId = "group-A", ...)

// Consumer 2 (same app, different class)
@KafkaListener(groupId = "group-A", ...)  // same group!
```

If both consumers are in the same group, Kafka assigns partitions between them. **Each message is only delivered to ONE of them.** If they're supposed to both process every message (e.g., one for DB write, one for analytics), they need **different group IDs**.

---

### Mistake 3: Throwing checked exceptions from `@KafkaListener` without `throws Exception`

```java
// BAD
public void onAssignRole(AssignRoleEvent event) {
    // if this throws a checked exception, it's silently swallowed!
}

// GOOD — your code
public void onAssignRole(AssignRoleEvent event) throws Exception {
    // checked exceptions propagate to the error handler correctly
}
```

---

### Mistake 4: Long-running operations in the consumer without timeout

```java
public void onAssignRole(AssignRoleEvent event) throws Exception {
    callExternalAPI(event);  // what if this hangs for 30 minutes?
}
```

Kafka's `max.poll.interval.ms` (default: 5 minutes) is how long between poll() calls before Kafka considers the consumer dead and triggers a rebalance. Long operations = accidental rebalance = duplicate processing.

Fix: Set `max.poll.interval.ms` appropriately, or offload long operations to a separate thread pool.

---

### Mistake 5: `replicas > available brokers` in prod with no ISR config

```java
TopicBuilder.name(topic).replicas(3).build()
```

If your Kafka cluster has 2 brokers, this fails at topic creation. Always verify broker count matches your replica config.

---

### Mistake 6: Not handling `whenComplete` failures

```java
// BAD
kafkaTemplate.send(topic, event);  // fire and forget, no callback
```

If the publish fails (network issue, broker down), you'll never know. The HTTP 202 was already sent. The event is **lost**.

```java
// GOOD — your code
kafkaTemplate.send(...).whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("Failed to publish...", ex);  // at minimum, log it!
    }
});
```

In production, you'd also want to write the failed event to a fallback (DB queue) here.

---

## 19. Glossary

| Term | Definition |
|------|-----------|
| **Topic** | Named channel for messages. Like a table in a DB but for events. |
| **Partition** | Sub-division of a topic. Unit of parallelism and ordering. |
| **Offset** | Sequential ID of a message within a partition. Never decreases. |
| **Producer** | Writes messages to Kafka. |
| **Consumer** | Reads messages from Kafka. |
| **Consumer Group** | Team of consumers sharing partitions of a topic. |
| **Broker** | A Kafka server. |
| **Bootstrap servers** | Initial connection points (not all brokers need to be listed). |
| **Rebalancing** | Reassignment of partitions among consumers in a group (on join/leave). |
| **ISR** | In-Sync Replicas — brokers that are fully caught up with the leader. |
| **acks** | How many brokers must confirm a write before producer considers it done. |
| **Idempotence** | Producing the same message multiple times = same result (no duplicates). |
| **At-least-once** | Default Kafka guarantee — you might get duplicates but won't lose messages. |
| **Exactly-once** | Advanced guarantee — each message processed exactly once (requires transactions). |
| **DLT** | Dead Letter Topic — where permanently-failed messages go. |
| **KRaft** | Kafka's built-in Raft-based metadata management (replaces ZooKeeper). |
| **Lag** | How many messages behind the consumer is from the latest offset. |
| **Key** | Optional field that determines which partition a message goes to. |
| **Serializer** | Converts Java object → bytes for Kafka transport. |
| **Deserializer** | Converts bytes → Java object on the consumer side. |
| **Schema Registry** | Central repository for message schemas (Avro/Protobuf). |
| **Commit** | Saving the current offset so future consumer starts know where to begin. |
| **Auto-commit** | Kafka periodically commits offsets automatically (dangerous for critical data). |
| **FixedBackOff** | Retry strategy: wait a fixed time between each retry attempt. |
| **ExponentialBackOff** | Retry strategy: wait increases exponentially (1s, 2s, 4s, 8s...). |
| **NonRetryable** | An error that will never be fixed by retrying (e.g. employee doesn't exist). |

---

## Summary — The 10 Things to Never Forget

```
1. ENABLE_AUTO_COMMIT = false         → Never lose a message
2. AUTO_OFFSET_RESET = earliest       → Never miss a message when consumer restarts  
3. acks = all + idempotence = true    → Never duplicate on the producer side
4. Key by entity ID                   → Ordering per entity, no race conditions
5. Consumer must be idempotent        → Handle duplicates from rebalances
6. Separate retryable vs non-retryable → Don't retry forever on permanent failures
7. Log eventId on every log line      → Trace the full lifecycle of any event
8. Validate AGAIN in consumer         → Producer pre-check is just an optimization
9. Don't block the producer thread    → Use whenComplete(), not .get()
10. Partition count = max parallelism → Choose wisely, can't shrink later
```

---

*This tutorial is written against your actual `spring_rc` codebase. Every config property, class name, and flow diagram maps directly to code you can open and read right now.*

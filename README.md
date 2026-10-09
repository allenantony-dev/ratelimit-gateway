# ratelimit-gateway

A rate-limiting reverse proxy, written in Go. I built it to learn how a gateway protects a backend it doesn't control.

The gateway sits in front of a slow service and passes requests through. The proxy caps how many requests it will handle at once, which keeps it alive under a flood. But that cap is global, so a single client looping as fast as it can fills every slot, and a hundred well-behaved clients get turned away for a greedy one they've never heard of.

That's the shape of the whole thing. There are two kinds of overload here, and they look alike until you try to fix them. One is *too many requests at once*, which exhausts the gateway itself: memory, goroutines, sockets. The other is *one client taking more than its share*, which starves everyone else while the gateway is otherwise fine. The concurrency cap above handles the first and does nothing for the second. Fairness is a separate mechanism, keyed per client, layered on top, and keeping the two straight is most of the difficulty.

Each section below is one way a naive version broke, in the order I fixed it, with a link to the commit. The last section is the exception: three bits of production hygiene that hadn't broken anything yet but would have.

**The failures, in order:**

1. [A burst of traffic kills the proxy](#a-burst-of-traffic-kills-the-proxy)
2. [One client eats the whole gateway](#one-client-eats-the-whole-gateway)
3. [Clients retry blindly](#clients-retry-blindly)
4. [The reset edge lets 2x through](#the-reset-edge-lets-2x-through)
5. [The sliding window is exact, and that costs](#the-sliding-window-is-exact-and-that-costs)
6. [A second proxy doubles every limit](#a-second-proxy-doubles-every-limit)
7. [The limiter now depends on Redis](#the-limiter-now-depends-on-redis)
8. [The proxy wasn't forwarding the request](#the-proxy-wasnt-forwarding-the-request)
9. [A hung backend pins every slot](#a-hung-backend-pins-every-slot)
10. [One limit for every route](#one-limit-for-every-route)
11. [Changing a limit meant a redeploy](#changing-a-limit-meant-a-redeploy)
12. [The unglamorous production gaps](#the-unglamorous-production-gaps)

## How it works

Three programs and a Redis:

```
  many clients               the gateway                 one slow backend
 (each a source IP)  ─────▶   proxy :8080   ─────▶   upstream :9000
                               │      │
                    concurrency cap   per-client, per-route
                    (global, 503)     rate limit (429)
                               │
                            Redis  ← shared limiter state
```

| Program | What it does |
|---|---|
| `proxy` | The gateway. Caps total concurrency, rate-limits each client per route, forwards the rest. Everything interesting lives here. |
| `upstream` | A stand-in backend on `:9000`. It echoes back the request it received (method, path, query, headers, body), so you can see exactly what the proxy forwarded. |
| `client` | A load generator. Poses as many source IPs, fires N requests, and honors `Retry-After` with jittered backoff, so you can watch the limiter from the client's side. |

Every request runs the same gauntlet inside the proxy. The rate limit runs first even though it's the expensive check (a Redis round trip against a channel send), because a rejected request should still spend a token, so the limiter measures how often a client *asks*, not how often it's served:

```
request ─▶ rate limit (per route + IP, token bucket in Redis)
             │ over quota ─▶ 429 + Retry-After
             ▼ ok
           concurrency cap (global, 100 slots)
             │ full ─▶ 503
             ▼ slot acquired
           forward to upstream (5s timeout)
             │ timed out ─▶ 504   broke ─▶ 502   client gone ─▶ drop
             ▼ ok
           stream the response back
```

The `upstream` started life as a service that slept for ten seconds, because slowness is what makes requests pile up and overload visible. It became an echo later, once I needed to see what the proxy was actually forwarding ([section 8](#the-proxy-wasnt-forwarding-the-request)).

## The design, problem by problem

### A burst of traffic kills the proxy

[`de97d08`](https://github.com/allenantony-dev/ratelimit-gateway/commit/de97d08) · [`a7ccda1`](https://github.com/allenantony-dev/ratelimit-gateway/commit/a7ccda1)

The first version had no limits at all: take every request, forward it, wait. The backend is slow. That's the premise of a gateway, you don't get to pick how fast the thing behind you is. So a few thousand requests arriving together all sit inside the proxy at once, each holding a goroutine and its buffers, waiting on a backend that answers in its own time. Memory climbs until the kernel kills the process.

The fix isn't to go faster, it's to refuse work you can't do. The whole mechanism is a buffered channel of 100 empty structs: take a slot before forwarding, hand it back after. If the channel is full there's no slot, and the request gets a `503` immediately, not queued. A queue would just move the memory problem somewhere less visible; refusing is honest backpressure. `maxInFlight` requests run, and the next one is turned away in microseconds instead of stalling.

> Crashing it on purpose was harder than I expected. I ran the proxy inside a memory-capped cgroup (`systemd-run --user --scope -p MemoryMax=512M`) so only the proxy died and never my machine. Even then it held until around 9,500 concurrent requests, and the number wasn't stable: a run right after the laptop woke from hibernation wouldn't crash at all, however hard I pushed. The server not crashing didn't mean it was safe. It meant I hadn't pushed hard enough yet.

### One client eats the whole gateway

[`2eeacf7`](https://github.com/allenantony-dev/ratelimit-gateway/commit/2eeacf7)

The concurrency cap is indifferent to *who* is calling. One client looping flat-out fills all 100 slots, and every other client gets `503`s it did nothing to earn. The cap protects the gateway; it does nothing for fairness.

So a second limit, per client, keyed by source IP. The first version was the obvious one, a fixed-window counter: count a client's requests, reset the count every minute. It works, and it has a well-known bug that gets its own section below.

### Clients retry blindly

[`29670a6`](https://github.com/allenantony-dev/ratelimit-gateway/commit/29670a6) · [`663282c`](https://github.com/allenantony-dev/ratelimit-gateway/commit/663282c)

A rejected client needs two things a bare `429` doesn't give it: how much quota it has, and when to come back. Without them it does the worst possible thing: retries instantly, in a tight loop, turning one rejection into a hundred.

The proxy now sets `X-RateLimit-Limit`, `-Remaining` and `-Reset`, and `Retry-After` on a `429`. The client honors them: wait `Retry-After` on a `429`, a short default on a `503`, both with full jitter so a crowd of rejected clients doesn't retry in lockstep and stampede the next window the instant it opens.

> `X-RateLimit-Reset` is seconds-until-reset here, not a Unix timestamp. GitHub's API uses the timestamp form and a lot of clients assume it, but the draft I followed used a duration, chosen so the client's clock and the server's clock don't have to agree on what time it is. (The spec has since moved on: the current revision drops the separate reset field and folds the remaining count and the window into a single `RateLimit` field.) The duration reasoning is what I wanted, so that's what I kept.

### The reset edge lets 2x through

[`794e283`](https://github.com/allenantony-dev/ratelimit-gateway/commit/794e283)

A fixed window resets on a cliff. A client can spend its whole quota in the last second of one window and its whole quota in the first second of the next, twice the limit in a two-second span straddling the boundary. Against a 10-per-minute limit I watched a client land about 18 requests in roughly six seconds and the counter was happy the whole time, because no single window ever saw more than 10.

A sliding window removes the cliff. Instead of a count that resets, keep the timestamps of a client's recent admitted requests; on each new request drop the timestamps older than a minute and count what's left. Nothing resets all at once; old requests age out one at a time, so it's never more than the limit in any rolling 60-second stretch.

### The sliding window is exact, and that costs

[`6ac744c`](https://github.com/allenantony-dev/ratelimit-gateway/commit/6ac744c)

The sliding window is exact, but exactness isn't free. It stores a timestamp per admitted request, so memory is O(limit) per client, and it hands quota back in the same clumpy shape the traffic arrived in. It also forbids bursts outright: a client idle for an hour still can't make 11 quick calls, which real clients find maddening.

A token bucket trades a little exactness for both. Store one number per client, a token balance, that refills continuously at the limit's rate, capped at a burst size. A request spends a token; no tokens, no entry.

| | sliding window | token bucket |
|---|---|---|
| per client | timestamps of recent admits | one balance + last-updated time |
| memory | O(limit) | O(1) |
| bursts | none; strict ≤ limit per window | up to the burst, then the steady rate |
| worst case in a window | the limit | ~2× (full bucket + a window's refill) |
| release shape | clumpy, mirrors arrivals | smooth, steady cadence |

The bucket is cheaper, smoother, and burst-friendly; the price is that over any single window you can see up to ~2× (a full bucket spent at once, plus what refills during the window). That looseness is usually the right trade, and it's the version the gateway runs.

> The one line I had to work out on paper was the refill: `tokens = min(burst, tokens + (now − last) × rate)`. Time elapsed since the last request times the refill rate is how many tokens dripped back; the `min` against `burst` is what stops an idle client from hoarding an unbounded burst. Everything else about the bucket falls out of that line.

### A second proxy doubles every limit

[`15e1ffa`](https://github.com/allenantony-dev/ratelimit-gateway/commit/15e1ffa) · [`ccfc992`](https://github.com/allenantony-dev/ratelimit-gateway/commit/ccfc992)

One proxy keeps its limiter table in its own memory. Run two behind one address, which is what you do the moment there's real traffic, and a single client gets the full limit from *each*, because neither instance knows the other exists. 10 + 10 = 20. The client's `first-pass` counter (requests that got a `200` on the first try) is what exposed it: that number should never exceed the limit, and it was sitting at about twice it.

The fix is shared state. The token bucket moves into Redis, and the whole read-refill-take step runs as one Lua script. Redis is single-threaded and a script is one indivisible command, so two proxies hitting the same key can't interleave and race. The clock comes from Redis's own `TIME` too, so the proxies don't have to agree on the time themselves. Idle keys set an `EXPIRE` of `ceil(burst / rate)` seconds, one window of inactivity, so the table cleans up after itself instead of growing forever. The key at this point is `rl:<ip>`; the route joins it two sections down.

You can see it in the client's `first-pass` count (requests served on the very first try), firing 40 across two instances at a limit of 10:

```
two proxies, in-memory limiter (before ccfc992):   first-pass=20
two proxies, shared Redis limiter (current):       first-pass=10
```

Twenty is the bug: each instance granted the full 10 on its own. Ten is the fix: one bucket, counted once, wherever the request lands. The 20 only reproduces on the pre-Redis build; current main always uses Redis (and fails open to 40, not 20, if Redis is down), so what you can run today is the 10, in [Running it](#two-demos-to-run).

### The limiter now depends on Redis

[`ef33f34`](https://github.com/allenantony-dev/ratelimit-gateway/commit/ef33f34)

Shared state bought correctness and added a dependency. So: Redis is down at 2am. What should the gateway do?

It fails open. A Redis error admits the request with no limit applied, because the only thing Redis protects is per-client fairness, and the concurrency cap (which needs no Redis) still stands between a flood and the backend. Losing fairness for a few minutes is survivable; taking the whole gateway down because the limiter's datastore blinked is not. A genuinely sensitive route might choose the opposite and fail closed, but that's a deliberate per-route call, not the default. The Redis call gets a tight 50ms deadline, so a *slow* Redis can't quietly become the latency the limiter was supposed to prevent.

The trap with failing open is that it's silent: you can't fix what you can't see. So the proxy logs the transition, not the attempt: one line when Redis goes from up to down, one when it recovers, gated by a compare-and-swap on an atomic bool so a storm of failing requests still produces exactly one line.

> My first cut of that logging drowned in noise I hadn't written. go-redis logs every failed dial internally, around 27 lines per outage, which buried my tidy up/down messages completely. The fix was `redis.SetLogger` with a no-op logger: I report the state change myself, and the library keeps quiet about its retries.

### The proxy wasn't forwarding the request

[`64bb0dd`](https://github.com/allenantony-dev/ratelimit-gateway/commit/64bb0dd) · [`cac65e6`](https://github.com/allenantony-dev/ratelimit-gateway/commit/cac65e6)

Somewhere in all the limiter work I'd been testing with `GET /hello` and never noticed the proxy was forwarding almost nothing. It used `http.Get`, so every forwarded request came out as a `GET` to the right path and nothing else: method, query string, body, and headers all dropped on the floor.

I made the upstream echo back exactly what it received, and the gap was impossible to miss: a `POST /search?q=cats` with a JSON body arrived at the backend as a bare `GET /search`. The fix builds the outbound request by hand with `http.NewRequestWithContext`: same method, `RequestURI()` so the query survives, the original body, a cloned header set, and `ContentLength` copied across so the upstream knows there's a body to read.

> Building the echo before building the fix wasn't an accident. I wanted the bug on screen first, so that when I changed the forwarding I could watch the echo change too and know the fix had actually done something, instead of trusting that it had.

### A hung backend pins every slot

[`4cbbcc8`](https://github.com/allenantony-dev/ratelimit-gateway/commit/4cbbcc8)

The concurrency cap assumes slots come back. If the backend doesn't just slow down but *hangs*, the request holding a slot never finishes and the slot never returns. A hundred hung requests fill all hundred slots, and from then on every client gets a `503`, forever, even though the gateway itself is idle. The cap built to protect the gateway becomes the thing taking it down.

Each forward now runs under a 5-second timeout tied to the client's own request context. Two things can free the slot: the timeout firing, or the client hanging up (either one cancels the context). And the single error `Do` returns gets split into its three real causes: the deadline passed → `504`, the client cancelled → drop the response silently because nobody's listening, anything else → `502`. `errors.Is` unwraps the `url.Error` to tell them apart.

One thing a streaming proxy can't do: once it has sent `200` and started copying the body, it can't take the status back if the upstream stalls mid-stream; the client just gets a truncated response. The only way to preserve that option is to buffer the whole response before sending a status, which is exactly the unbounded-memory habit the first section removed. Flat memory is worth a truncated body on a rare mid-stream stall.

### One limit for every route

[`5f3c698`](https://github.com/allenantony-dev/ratelimit-gateway/commit/5f3c698)

`/login` and `/search` had been sharing one 10-per-minute limit, which fits neither. `/login` takes a password, so 10 guesses a minute is far too generous for someone brute-forcing it. `/search` is cheap, and a real user might hit it 60 times a minute without meaning anything by it. One number can't be both.

Limits become per-route: a map from path to limit, with a default for anything unlisted, which also quietly covers a route someone adds and forgets to configure. The Redis key gains the route (`rl:<route>:<ip>`), or the two routes would share a bucket and the per-route limits would do nothing.

Then a subtler problem, which took four tries to fully close: the limiter and the backend have to agree on what "the path" is, and any spelling they disagree on is a way around the strict limit.

> This bug kept coming back in a new disguise. I started with prefix matching, and `/login` quietly matched `/loginhistory`. I switched to exact matching and cleaned the incoming path with `path.Clean`, so `/x/../login` and `//login` couldn't dodge the strict limit by spelling the route differently. Then the config file had the identical hole one level up: a key written as `/search/` (trailing slash) cleaned to something no cleaned request path would ever equal, a dead entry that silently handed `/search` the lax default. So I cleaned the config keys too, and *then* a key of `login` with no leading slash was still a dead entry, because every real request path starts with one. Each fix was a single line. The lesson was the same one four times: **if the thing choosing the limit and the thing routing the request normalize paths differently, the difference between them is a bypass.**

With the limits in `config.json`, the three cases come out right:

```
/login   -> X-RateLimit-Limit: 5
/search  -> X-RateLimit-Limit: 60
/hello   -> X-RateLimit-Limit: 10   (unlisted, so the default)
```

### Changing a limit meant a redeploy

[`b80fce3`](https://github.com/allenantony-dev/ratelimit-gateway/commit/b80fce3)

The limits were a map compiled into the binary. Picture the 2am version of that: one client is hammering `/search`, you want to drop its limit to 5, and the only path is edit Go, rebuild, redeploy. That's the wrong amount of work for changing a number.

The limits move into a JSON file the proxy re-reads every 10 seconds. The part that matters is *how* the swap happens. Request handlers read the map constantly, so you can't mutate it in place (Go detects a concurrent map read and write and kills the process on purpose, it's not undefined), and you don't want handlers blocking on a reload lock either. Instead the reloader builds a brand-new map off to the side where nobody can see it, then publishes it with one atomic pointer store. Readers do a lock-free atomic load and get either the whole old config or the whole new one, never a half-written map.

Two asymmetries I had to decide, and they point opposite ways:

- **A bad file on reload** keeps the last good config and logs loudly. A fat-fingered edit should never take down a running gateway.
- **A bad file at startup** refuses to start. There's no last-good to fall back on, and silently running on defaults would be worse than not running: you'd think your limits were applied when they weren't.

Fail hard at startup, fail soft on reload. The keys are cleaned and validated (positive limits, leading slash, no post-clean duplicates) the same way at both, and a reload only logs when the config actually changed, compared with `maps.Equal`, so you get a confirmation line when something took effect and silence when it didn't.

### The unglamorous production gaps

[`6dcdf63`](https://github.com/allenantony-dev/ratelimit-gateway/commit/6dcdf63)

Three things that aren't clever but are the difference between a demo and something you'd leave running.

**Graceful shutdown.** `ListenAndServe` has no off switch, so Ctrl-C killed in-flight proxied responses mid-write. Now `SIGINT`/`SIGTERM` stops accepting new connections and `srv.Shutdown` lets the requests already in flight finish first. An in-flight upstream call is capped at 5 seconds, so most drain well within the 10-second shutdown deadline. The exception is a client slowly draining a large response: that's bounded by the 15-second `WriteTimeout`, which is longer than the shutdown deadline, so there it's `Shutdown` that cuts the connection, not the write timeout.

**Server timeouts.** The inbound side had none, which is slowloris bait: a client opens a connection, dribbles its request headers one byte at a time, and holds a slot forever. `ReadHeaderTimeout` closes that. I set a `WriteTimeout` as well but deliberately *no* `ReadTimeout`: that would cap the time to read the entire request body, which is wrong for a proxy whose whole job can be forwarding a large upload.

**Structured logging** (`slog`), because on a gateway you grep by route and client IP. Rejections log at the level their severity warrants: a `429` (one client over its quota) at `Info`, a `503` (the whole concurrency cap saturated, worse news) at `Warn`, a `502`/`504` (the backend broke) at `Error`, the last two carrying the route and client. What I didn't add was a per-client "already warned" table to dedupe the `429`s; that would quietly rebuild the unbounded per-client state the whole project was about getting rid of. The level split does the filtering instead, for free.

## Running it

You need Redis. The simplest way:

```bash
docker run --rm -d --name rl-redis -p 6379:6379 redis
```

Then the three programs, each in its own terminal (or backgrounded):

```bash
go run upstream/main.go        # echo backend on :9000
go run proxy/main.go           # gateway on :8080, reads ./config.json
go run client/main.go 50       # fire 50 requests from one source IP
```

`config.json` holds the per-route limits. Edit it while the proxy is running and the change takes effect within 10 seconds, no restart:

```json
{
  "/login": 5,
  "/search": 60
}
```

### Knobs

**proxy**

| | |
|---|---|
| `-addr` | listen address (default `:8080`) |
| `-config` | per-route limits file (default `config.json`) |
| `REDIS_ADDR` env | Redis address (default `127.0.0.1:6379`) |

**client**

| | |
|---|---|
| positional count | how many requests to fire (default 25) |
| `-from <ip>` | source IP to dial from, e.g. `127.0.0.2`. Loopback (`127.0.0.0/8`) gives you a pile of distinct client IPs on one machine, which is how you test fairness between clients. |
| `PROXIES` env | comma-separated proxy URLs, round-robined across requests, for the two-instance demo |

The client prints a summary at the end: `served` (and how many on the first try), `gave-up`, `failed`, and total `retries`. That's where the limiter's behavior shows up from the outside.

### Two demos to run

**Seeing the limit hold across two proxies** ([section 6](#a-second-proxy-doubles-every-limit)). Run two instances sharing one Redis and point the client at both. The `first-pass` count stays at the limit instead of doubling, because the bucket lives in Redis now and not in either process:

```bash
go run proxy/main.go -addr :8080 &
go run proxy/main.go -addr :8081 &
PROXIES=http://127.0.0.1:8080,http://127.0.0.1:8081 go run client/main.go 40
```

**The crash** ([section 1](#a-burst-of-traffic-kills-the-proxy)). Point the proxy at a backend that hangs instead of echoing, run it under a memory cap so only the proxy can die, and fire enough concurrent requests to pile up inside it. Keep this on localhost and keep the cgroup cap on; the whole point is to watch the proxy die without taking anything else with it.

## Known gaps

Deliberately deferred, with the trigger for each.

**When there's real traffic**
- The token bucket admits up to ~2× over a single window by design (full bucket plus a window's refill). If a route needs strict "never more than N per window," the sliding window is the trade to make for it.
- One global concurrency cap across all routes, so a flood of cheap `/search` calls can fill the slots an expensive `/login` would want. The fix is a per-route (or per-class) cap.
- Nothing caps the size of a body streamed through; a huge upload or response still flows (memory per copy is bounded, the total transfer isn't).
- Because the rate limit is charged on arrival, ahead of the concurrency cap, every request pays its Redis round trip first, including ones about to be refused. Under a flood, each shed `429` and `503` still costs a Redis call before the turn-away. The 50ms budget bounds it, but it's work the gateway does in order to reject work.

**Before exposing it publicly**
- The upstream address is hardcoded to `http://127.0.0.1:9000`; it wants to be configurable, and really wants a small routing table.
- No TLS: the gateway speaks plain HTTP.
- The client IP comes straight from `RemoteAddr`, so behind a load balancer every client collapses to one IP and the per-client limit becomes useless. It would need to trust `X-Forwarded-For` from known hops.
- No admin API: changing a limit means editing JSON on the box. (It reloads without a restart, at least.)

**Small blast radius**
- Fail-open is all-or-nothing for the whole proxy; there's no per-route fail-*closed* option yet, which a login route would want.
- The reload re-reads and re-parses the file every 10s even when it hasn't changed. An mtime check would skip the parse, but the parse is cheap, so it hasn't earned the code.
- `reloadInterval` and the upstream timeout are constants, not flags.

**Not built**
- No metrics endpoint: request counts, `429`/`503`/`504` rates. The logs are the only visibility.
- No circuit breaker on a consistently failing upstream: every request still tries and waits out its 5 seconds rather than failing fast once the backend is clearly down.

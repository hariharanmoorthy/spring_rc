# Learn: Docker Compose & NGINX for Beginners

---

## 1. What is Docker Compose?

Docker Compose lets you **define and run multiple containers** using a single file called `docker-compose.yml`.

Instead of running 7 separate `docker run` commands manually, you write everything once and just run:
```bash
podman compose up
```

---

## 2. docker-compose.yml — Line by Line

```yaml
services:          # List of all containers you want to run
  postgres:        # This is just a NAME you give to the service (can be anything)
    image: postgres:15       # Use this pre-built image from Docker Hub
    container_name: spring_rc_postgres   # The actual container name inside podman
    environment:             # Environment variables passed into the container
      POSTGRES_DB: springlearn
      POSTGRES_USER: hari
      POSTGRES_PASSWORD: hari
    ports:
      - "5432:5432"          # HOST_PORT:CONTAINER_PORT
                             # Left side = your Mac, Right side = inside container
    volumes:
      - postgres_data:/var/lib/postgresql/data
      # postgres_data = a named volume (explained below)
      # /var/lib/postgresql/data = where postgres stores its data INSIDE the container
    networks:
      - app_network          # Connect this container to a virtual network
    healthcheck:             # Check if postgres is ready before starting app
      test: ["CMD-SHELL", "pg_isready -U hari -d springlearn"]
      interval: 5s           # Check every 5 seconds
      retries: 10            # Try 10 times before giving up
```

---

## 3. Ports Explained — HOST:CONTAINER

```
ports:
  - "8080:8080"
```

```
Your Mac (browser) ──► localhost:8080 ──► Container's port 8080
```

- **Left (8080)** = port on YOUR machine (what you type in browser)
- **Right (8080)** = port INSIDE the container

Example:
```
- "9090:8080"   # You open localhost:9090, it goes to container's 8080
```

---

## 4. Volumes — Saving Data Permanently

### The Problem:
When a container stops or is deleted, **all data inside is LOST**.

### The Solution: Volumes
```yaml
volumes:
  - postgres_data:/var/lib/postgresql/data
```

This means:
- Take the folder `/var/lib/postgresql/data` inside the container
- Save it to a named volume called `postgres_data` on your host machine
- Even if the container is deleted, the data stays!

```
Container ──► /var/lib/postgresql/data ──► saved to ──► postgres_data (on your Mac)
```

### Declaring the volume at the bottom:
```yaml
volumes:
  postgres_data:   # Just declaring "hey, create this named volume"
```

This is like registering the volume so Docker/Podman knows it exists.

### Named Volume vs Bind Mount:
| Type | Example | Use case |
|------|---------|---------|
| Named Volume | `postgres_data:/var/lib/postgresql/data` | Databases, persistent data |
| Bind Mount | `./nginx.conf:/etc/nginx/nginx.conf` | Config files from your code folder |

`./nginx.conf` means "use THIS file from my current folder" (bind mount).

---

## 5. Networks — How Containers Talk to Each Other

By default, containers are **isolated** — they can't talk to each other.

A **network** is like a private Wi-Fi that connects your containers.

```yaml
networks:
  - app_network   # Join this virtual network
```

At the bottom:
```yaml
networks:
  app_network:
    driver: bridge   # bridge = most common, creates a virtual network
```

### Why service name becomes the hostname:
Inside the network, each container is reachable by its **service name**.

So if your app needs to connect to postgres:
```
DB_HOST: postgres   # Not an IP, just the service name!
```

Podman/Docker internally resolves `postgres` → the postgres container's IP.

---

## 6. Network Drivers

| Driver | What it does |
|--------|-------------|
| `bridge` | Default. Creates an isolated virtual network on your machine. Containers on same bridge can talk. |
| `host` | Container shares your Mac's network directly. No isolation. |
| `none` | No network at all. Completely isolated. |

For learning, always use `bridge`.

---

## 7. `depends_on` — Start Order

```yaml
app:
  depends_on:
    postgres:
      condition: service_healthy   # Wait until postgres healthcheck passes
```

Without this, your app might start before postgres is ready and crash.

---

## 8. NGINX — What is it?

NGINX is a **web server / reverse proxy / load balancer**.

In our case, we use it as a **Load Balancer**:

```
Browser ──► NGINX (port 8080) ──► routes to ──► app1, app2, app3, app4, app5
```

Instead of hitting your app directly, all requests go to NGINX first, and NGINX decides which app server handles it (Round Robin = one by one in order).

---

## 9. nginx.conf — Line by Line

```nginx
events {}   # Required block, handles connection settings. Leave it empty for defaults.

http {      # All web traffic configuration goes here

    upstream app_servers {          # Define a GROUP of backend servers
        server spring_rc_app_1:8080;   # app server 1
        server spring_rc_app_2:8080;   # app server 2
        server spring_rc_app_3:8080;   # app server 3
        server spring_rc_app_4:8080;   # app server 4
        server spring_rc_app_5:8080;   # app server 5
        # Default behaviour = Round Robin (no extra config needed!)
    }

    server {          # Define a virtual server
        listen 8080;  # NGINX listens on port 8080

        location / {  # For ALL requests (/)
            proxy_pass http://app_servers;   # Forward to the upstream group

            # Pass original request info to the app:
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;

            # Add a header in response so YOU can see which server handled it:
            add_header X-Upstream-Server $upstream_addr always;
        }
    }
}
```

---

## 10. How Round Robin Works

```
Request 1 ──► spring_rc_app_1
Request 2 ──► spring_rc_app_2
Request 3 ──► spring_rc_app_3
Request 4 ──► spring_rc_app_4
Request 5 ──► spring_rc_app_5
Request 6 ──► spring_rc_app_1  (back to start)
...
```

NGINX automatically does this with no extra config.

---

## 11. How to See Which Server Handled Your Request

Every response from NGINX includes a header `X-Upstream-Server`.

Run this in terminal:
```bash
curl -s -I http://localhost:8080/api/v1/employees
```

Look for this line in the output:
```
X-Upstream-Server: 10.89.0.5:8080
```

Each server has a different internal IP — that's how you know which one handled it.

Or hit the URL multiple times and watch the IP change:
```bash
for i in {1..5}; do curl -s -I http://localhost:8080/api/v1/employees | grep X-Upstream; done
```

---

## 12. Scaling with Podman Compose

Instead of defining 5 services manually, use:
```bash
podman compose up --scale app=5
```

This spins up 5 containers named: `spring_rc_app_1` ... `spring_rc_app_5`

> ⚠️ Do NOT set `container_name` in the `app` service if you use `--scale`. Fixed names cause conflicts when making multiple copies.

---

## 13. Full Flow Summary

```
You (browser)
     │
     ▼
localhost:8080
     │
     ▼
NGINX container (spring_rc_nginx)
     │  reads nginx.conf
     │  round-robins between:
     ├──► spring_rc_app_1:8080
     ├──► spring_rc_app_2:8080
     ├──► spring_rc_app_3:8080
     ├──► spring_rc_app_4:8080
     └──► spring_rc_app_5:8080
               │
               ▼
        postgres container
        (data saved in postgres_data volume)
```

All containers talk to each other over the `app_network` bridge network.


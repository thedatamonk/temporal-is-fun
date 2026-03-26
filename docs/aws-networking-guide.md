# AWS Networking Concepts — A Practical Guide

A guide to the AWS networking concepts used in this project, explained through analogies and diagrams.

## The Big Picture

```
┌─────────────────────────────────────────────────────────────┐
│                        AWS Cloud                            │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │                    VPC (Your Private Network)         │  │
│  │                    10.0.0.0/16                        │  │
│  │                                                       │  │
│  │  ┌─────────────────┐    ┌──────────────────────────┐  │  │
│  │  │  Public Subnet  │    │    Private Subnet        │  │  │
│  │  │  10.0.1.0/24    │    │    10.0.2.0/24           │  │  │
│  │  │                 │    │                          │  │  │
│  │  │  ┌───────────┐  │    │  ┌──────┐  ┌─────────┐  │  │  │
│  │  │  │    ALB    │──┼────┼─>│  EC2 │  │   RDS   │  │  │  │
│  │  │  └───────────┘  │    │  └──┬───┘  └────▲────┘  │  │  │
│  │  │                 │    │     │            │       │  │  │
│  │  └─────────────────┘    │     └────────────┘       │  │  │
│  │          ▲               └──────────────────────────┘  │  │
│  └──────────┼────────────────────────────────────────────┘  │
│             │                                               │
│    ┌────────┴────────┐                                      │
│    │ Internet Gateway │                                      │
│    └────────┬────────┘                                      │
└─────────────┼───────────────────────────────────────────────┘
              │
         The Internet
              │
         Your Browser
```

## VPC — Your Private Cloud Within the Cloud

A VPC (Virtual Private Cloud) is your own isolated network inside AWS. Think of it as a **gated neighborhood** — you control who gets in, what's connected, and how traffic flows.

```
┌──────────────────────────────────────┐
│           VPC = Gated Neighborhood   │
│                                      │
│   Your own IP range (10.0.0.0/16)    │
│   Your own routing rules             │
│   Your own access controls           │
│                                      │
│   Nothing gets in or out unless      │
│   you explicitly allow it.           │
└──────────────────────────────────────┘
```

Key points:
- Every AWS account gets a **default VPC** in each region (free, always there)
- For real projects, you create your own VPC for isolation
- The CIDR block (e.g., `10.0.0.0/16`) defines the range of private IP addresses available — `/16` gives you ~65,000 addresses

## Subnets — Streets in the Neighborhood

A subnet is a subdivision of your VPC. It's a **street within the neighborhood** — and the type of street determines what's possible.

```
VPC (Gated Neighborhood)
│
├── Public Subnet (street with highway exit)
│   - Has a route to the Internet Gateway
│   - Resources CAN get public IP addresses
│   - Accessible from the internet (with security group rules)
│
└── Private Subnet (internal street, no highway)
    - NO route to the Internet Gateway
    - Resources CANNOT get public IPs
    - Only reachable from within the VPC
```

### What we built (learning project)

```
VPC
└── Public Subnet (us-east-1a)
    └── EC2 instance (public IP: 98.84.42.232)
        ├── Temporal Server
        ├── Temporal UI
        ├── PostgreSQL
        ├── Workers
        └── Trigger
```

Everything in one public subnet — simple but less secure.

### What production looks like

```
VPC
├── Public Subnet (us-east-1a)
│   └── Load Balancer (only public-facing resource)
│
└── Private Subnet (us-east-1a)
    ├── EC2 instance (no public IP)
    └── RDS PostgreSQL (no public IP)
```

The EC2 and database are hidden behind the load balancer.

## IP Addresses and Ports

An IP address is a **building address**. A port is an **apartment number** inside that building.

```
EC2 Instance (98.84.42.232)
│
├── Port 22    → SSH           (remote terminal access)
├── Port 5432  → PostgreSQL    (database)
├── Port 7233  → Temporal Server (workflow engine gRPC)
├── Port 8080  → Temporal UI   (web dashboard)
│
│   One machine, one IP, multiple services.
│   The port number routes traffic to the right service.
│
│   ssh ec2-user@98.84.42.232       → knocks on apartment 22
│   http://98.84.42.232:8080        → knocks on apartment 8080
```

Common port numbers:

| Port | Service | Protocol |
|------|---------|----------|
| 22 | SSH | Remote terminal |
| 80 | HTTP | Web (unencrypted) |
| 443 | HTTPS | Web (encrypted) |
| 5432 | PostgreSQL | Database |
| 3306 | MySQL | Database |
| 7233 | Temporal | gRPC |
| 8080 | Various | Common for web UIs |

## Security Groups — The Bouncer

A security group is a **firewall** attached to a resource. It controls which traffic can come in (ingress) and go out (egress).

```
                     Security Group (Bouncer)
                     ┌─────────────────────┐
                     │  INGRESS RULES:     │
  Your IP ──────────>│  Port 22   ✅ ALLOW │──────> EC2
  (49.207.59.58)     │  Port 8080 ✅ ALLOW │
                     │                     │
  Random IP ────────>│  Port 22   ❌ BLOCK │
  (1.2.3.4)          │  Port 8080 ❌ BLOCK │
                     │                     │
  Anyone ───────────>│  Port 7233 ❌ BLOCK │  (no rule = blocked)
                     │  Port 5432 ❌ BLOCK │
                     └─────────────────────┘
```

Key rules:
- **Default: everything is blocked** — you must explicitly allow traffic
- Rules can reference IP addresses OR other security groups
- Every EC2 instance must have a security group (both public and private subnets)

### Our project's security group

```hcl
# What we configured in Terraform:
ingress {
  port        = 22
  cidr_blocks = ["49.207.59.58/32"]   # Only your IP
}
ingress {
  port        = 8080
  cidr_blocks = ["49.207.59.58/32"]   # Only your IP
}
egress {
  port        = 0        # All ports
  cidr_blocks = ["0.0.0.0/0"]         # Anywhere (so EC2 can download Docker images)
}
```

### Production security groups (referencing other security groups)

Instead of hardcoding IP addresses, production security groups reference each other:

```
┌─────────────────────┐     ┌────────────────────────┐     ┌──────────────────────┐
│   ALB               │     │   EC2                  │     │   RDS                │
│   SG: sg-aaa        │     │   SG: sg-bbb           │     │   SG: sg-ccc         │
│                     │     │                        │     │                      │
│ Ingress:            │     │ Ingress:               │     │ Ingress:             │
│  443 from 0.0.0.0/0│────>│  8080 from sg-aaa only │────>│  5432 from sg-bbb    │
│  (the whole internet│     │  (only the ALB)        │     │  only (only the EC2) │
│                     │     │                        │     │                      │
└─────────────────────┘     └────────────────────────┘     └──────────────────────┘
```

This means:
- Only the ALB accepts internet traffic
- Only the ALB can talk to EC2 (via security group reference)
- Only EC2 can talk to the database (via security group reference)
- If you add more EC2 instances with `sg-bbb`, they automatically get database access

## Internet Gateway — The Highway On-Ramp

An Internet Gateway connects your VPC to the internet. Without it, nothing in your VPC can reach the outside world.

```
The Internet
      │
┌─────┴─────┐
│  Internet  │
│  Gateway   │  ← Attached to the VPC, one per VPC
└─────┬─────┘
      │
┌─────┴──────────────────────────────────┐
│  VPC                                   │
│                                        │
│  ┌──────────────────┐  ┌────────────┐  │
│  │  Public Subnet   │  │  Private   │  │
│  │  Route: 0.0.0.0  │  │  Subnet    │  │
│  │  → IGW ✅        │  │  No route  │  │
│  │                  │  │  to IGW ❌  │  │
│  └──────────────────┘  └────────────┘  │
└────────────────────────────────────────┘
```

The **route table** is what makes a subnet public or private:
- Public subnet's route table: `0.0.0.0/0 → Internet Gateway` (all internet traffic goes through the IGW)
- Private subnet's route table: no such route (can't reach the internet directly)

## IAM Role — The Building's Access Badge

An IAM role gives an EC2 instance permission to use other AWS services (S3, SQS) without storing credentials on the machine.

```
┌──────────────────────┐
│  IAM Role            │
│  "churn-pipeline-    │
│   ec2-role"          │
│                      │
│  Permissions:        │
│  ├── S3: Get, Put,   │
│  │   List on bucket  │
│  └── SQS: Receive,   │
│      Delete messages │
└──────────┬───────────┘
           │ attached via Instance Profile
           ▼
┌──────────────────────┐
│  EC2 Instance        │
│                      │
│  Docker containers   │
│  call boto3 →        │
│  boto3 checks        │
│  instance metadata   │
│  (169.254.169.254)   │
│  → gets temporary    │
│  credentials from    │
│  the IAM role        │
│  → accesses S3/SQS   │
└──────────────────────┘
```

No AWS access keys stored on the machine. The instance metadata service provides temporary, auto-rotating credentials. This is the AWS best practice — never hardcode credentials.

## Load Balancer (ALB) — The Receptionist

An Application Load Balancer sits between the internet and your application. It handles:

```
User's Browser
      │
      │ HTTPS (port 443, encrypted)
      ▼
┌──────────────┐
│     ALB      │
│              │
│ - Terminates │  ← Handles HTTPS certificates
│   TLS/SSL    │
│ - Checks     │  ← Requires login / SSO
│   auth       │
│ - Health     │  ← Restarts unhealthy targets
│   checks     │
│ - Routes     │  ← Can distribute across multiple EC2s
│   traffic    │
└──────┬───────┘
       │ HTTP (port 8080, private network)
       ▼
┌──────────────┐
│     EC2      │
│  (private    │
│   subnet)    │
└──────────────┘
```

In our learning project, we skipped the ALB and accessed the EC2 directly. In production, the ALB adds security (HTTPS, authentication) and reliability (health checks, multiple targets).

## How It All Fits Together — Our Project

### What we deployed (learning version)

```
Your Machine
    │
    ├── terraform apply ──> Creates AWS resources
    │
    ├── ssh (port 22) ─────────────────────────────┐
    │                                               │
    ├── browser (port 8080) ───────────────────┐    │
    │                                          │    │
    └── aws s3 cp ──> S3 bucket ──> SQS queue  │    │
                                      │        │    │
                       ┌──────────────┼────────┼────┼──────────┐
                       │  VPC         │        │    │          │
                       │  ┌───────────┼────────┼────┼───────┐  │
                       │  │ Public    │        │    │       │  │
                       │  │ Subnet    ▼        ▼    ▼       │  │
                       │  │        ┌─────────────────────┐  │  │
                       │  │        │  EC2 (t3.xlarge)    │  │  │
                       │  │        │  ┌───────────────┐  │  │  │
                       │  │        │  │Docker Compose │  │  │  │
                       │  │        │  │  - Temporal   │  │  │  │
                       │  │        │  │  - PostgreSQL │  │  │  │
                       │  │        │  │  - Workers    │  │  │  │
                       │  │        │  │  - Trigger    │  │  │  │
                       │  │        │  └───────────────┘  │  │  │
                       │  │        └─────────────────────┘  │  │
                       │  │  Security Group:                │  │
                       │  │  Port 22, 8080 from your IP     │  │
                       │  └─────────────────────────────────┘  │
                       └───────────────────────────────────────┘
```

### What production would look like

```
Users
  │
  │ HTTPS (port 443)
  ▼
┌───────────────────────────────────────────────────────────────┐
│  VPC                                                         │
│  ┌────────────────────────┐   ┌───────────────────────────┐  │
│  │  Public Subnet         │   │  Private Subnet           │  │
│  │                        │   │                           │  │
│  │  ┌──────────────────┐  │   │  ┌─────────────────────┐  │  │
│  │  │  ALB             │──┼───┼─>│  EC2                │  │  │
│  │  │  - HTTPS         │  │   │  │  - Temporal Server  │  │  │
│  │  │  - Auth          │  │   │  │  - Temporal UI      │  │  │
│  │  │  - Health checks │  │   │  │  - Workers          │  │  │
│  │  └──────────────────┘  │   │  │  - Trigger          │  │  │
│  │                        │   │  └────────┬────────────┘  │  │
│  └────────────────────────┘   │           │               │  │
│                               │  ┌────────▼────────────┐  │  │
│                               │  │  RDS PostgreSQL     │  │  │
│                               │  │  - Auto backups     │  │  │
│                               │  │  - Persistent data  │  │  │
│                               │  └─────────────────────┘  │  │
│                               └───────────────────────────┘  │
│                                                               │
│  S3 Bucket ◄──── Event Notification ────► SQS Queue          │
└───────────────────────────────────────────────────────────────┘
```

## Quick Reference

| Concept | Analogy | Purpose |
|---------|---------|---------|
| VPC | Gated neighborhood | Isolated private network |
| Subnet | Street | Subdivides the VPC; public or private |
| Internet Gateway | Highway on-ramp | Connects VPC to internet |
| Route Table | Street signs | Directs traffic to the right destination |
| Security Group | Bouncer | Firewall controlling port/IP access |
| EC2 | Building | Virtual server running your code |
| Port | Apartment number | Routes traffic to the right service |
| IAM Role | Access badge | Grants AWS permissions without credentials |
| ALB | Receptionist | HTTPS, auth, health checks, load distribution |
| S3 | Storage warehouse | Object storage (files, artifacts) |
| SQS | Mailbox | Message queue between services |

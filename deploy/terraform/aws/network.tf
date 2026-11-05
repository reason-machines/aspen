# A dedicated VPC: public subnets for the load balancers (and the tasks when
# nat_gateway = false), private subnets for RDS (and the tasks when
# nat_gateway = true). S3 traffic uses a free gateway endpoint either way.

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.name }
}

# Strip every rule from the default security group so nothing uses it by
# accident.
resource "aws_default_security_group" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-default-unused" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = var.name }
}

resource "aws_subnet" "public" {
  count = var.az_count

  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[count.index]
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index)
  # Tasks get their public IP from the ECS service setting, not from here.
  map_public_ip_on_launch = false

  tags = { Name = "${var.name}-public-${local.azs[count.index]}" }
}

resource "aws_subnet" "private" {
  count = var.az_count

  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[count.index]
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index + 8)

  tags = { Name = "${var.name}-private-${local.azs[count.index]}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-public" }
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  count = var.az_count

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-private" }
}

resource "aws_route_table_association" "private" {
  count = var.az_count

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# One NAT gateway, not one per AZ: cheaper, at the cost of losing private
# subnet egress if its AZ fails. Only created when nat_gateway = true.
resource "aws_eip" "nat" {
  count = var.nat_gateway ? 1 : 0

  domain = "vpc"
  tags   = { Name = "${var.name}-nat" }
}

resource "aws_nat_gateway" "this" {
  count = var.nat_gateway ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id
  tags          = { Name = var.name }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route" "private_nat" {
  count = var.nat_gateway ? 1 : 0

  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this[0].id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${local.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id, aws_route_table.private.id]

  tags = { Name = "${var.name}-s3" }
}

# ---------------------------------------------------------------------------
# Security groups
# ---------------------------------------------------------------------------

resource "aws_security_group" "alb" {
  name_prefix = "${var.name}-alb-"
  description = "Aspen HTTPS load balancer"
  vpc_id      = aws_vpc.this.id
  tags        = { Name = "${var.name}-alb" }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "nlb" {
  count = var.enable_ssh ? 1 : 0

  name_prefix = "${var.name}-nlb-"
  description = "Aspen SSH load balancer"
  vpc_id      = aws_vpc.this.id
  tags        = { Name = "${var.name}-nlb" }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "tasks" {
  name_prefix = "${var.name}-tasks-"
  description = "Aspen ECS tasks"
  vpc_id      = aws_vpc.this.id
  tags        = { Name = "${var.name}-tasks" }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "db" {
  name_prefix = "${var.name}-db-"
  description = "Aspen PostgreSQL"
  vpc_id      = aws_vpc.this.id
  tags        = { Name = "${var.name}-db" }

  lifecycle {
    create_before_destroy = true
  }
}

# ALB: HTTP (redirect only) and HTTPS in from the allowed CIDRs, out to tasks.
resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  for_each = toset(var.http_ingress_cidrs)

  security_group_id = aws_security_group.alb.id
  description       = "HTTP, redirected to HTTPS"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
  cidr_ipv4         = each.value
}

resource "aws_vpc_security_group_ingress_rule" "alb_https" {
  for_each = toset(var.http_ingress_cidrs)

  security_group_id = aws_security_group.alb.id
  description       = "Git over HTTPS"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = each.value
}

resource "aws_vpc_security_group_egress_rule" "alb_to_tasks" {
  security_group_id            = aws_security_group.alb.id
  description                  = "To Aspen HTTP"
  ip_protocol                  = "tcp"
  from_port                    = var.http_port
  to_port                      = var.http_port
  referenced_security_group_id = aws_security_group.tasks.id
}

# NLB: SSH in from the allowed CIDRs; out to tasks for SSH and for the health
# check, which probes the HTTP port.
resource "aws_vpc_security_group_ingress_rule" "nlb_ssh" {
  for_each = var.enable_ssh ? toset(var.ssh_ingress_cidrs) : toset([])

  security_group_id = aws_security_group.nlb[0].id
  description       = "Git over SSH"
  ip_protocol       = "tcp"
  from_port         = var.ssh_public_port
  to_port           = var.ssh_public_port
  cidr_ipv4         = each.value
}

resource "aws_vpc_security_group_egress_rule" "nlb_to_tasks" {
  for_each = var.enable_ssh ? toset([tostring(var.ssh_port), tostring(var.http_port)]) : toset([])

  security_group_id            = aws_security_group.nlb[0].id
  description                  = "To Aspen port ${each.value}"
  ip_protocol                  = "tcp"
  from_port                    = tonumber(each.value)
  to_port                      = tonumber(each.value)
  referenced_security_group_id = aws_security_group.tasks.id
}

# Tasks: in only from the load balancers.
resource "aws_vpc_security_group_ingress_rule" "tasks_from_alb" {
  security_group_id            = aws_security_group.tasks.id
  description                  = "Git over HTTP from the ALB"
  ip_protocol                  = "tcp"
  from_port                    = var.http_port
  to_port                      = var.http_port
  referenced_security_group_id = aws_security_group.alb.id
}

resource "aws_vpc_security_group_ingress_rule" "tasks_from_nlb" {
  for_each = var.enable_ssh ? toset([tostring(var.ssh_port), tostring(var.http_port)]) : toset([])

  security_group_id            = aws_security_group.tasks.id
  description                  = "Port ${each.value} from the SSH NLB"
  ip_protocol                  = "tcp"
  from_port                    = tonumber(each.value)
  to_port                      = tonumber(each.value)
  referenced_security_group_id = aws_security_group.nlb[0].id
}

# Tasks: out to HTTPS (S3, image registry, CloudWatch, Secrets Manager) and to
# the database. DNS goes to the VPC resolver, which security groups don't
# filter.
resource "aws_vpc_security_group_egress_rule" "tasks_https" {
  security_group_id = aws_security_group.tasks.id
  description       = "HTTPS to AWS APIs and the image registry"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "tasks_to_db" {
  security_group_id            = aws_security_group.tasks.id
  description                  = "PostgreSQL"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.db.id
}

resource "aws_vpc_security_group_ingress_rule" "db_from_tasks" {
  security_group_id            = aws_security_group.db.id
  description                  = "PostgreSQL from Aspen tasks"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = aws_security_group.tasks.id
}

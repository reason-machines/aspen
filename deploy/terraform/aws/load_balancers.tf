# An ALB terminates TLS for Git over HTTPS; an NLB passes Git over SSH through
# as plain TCP. Both forward to the same ECS tasks.

# ---------------------------------------------------------------------------
# Certificate and DNS for the HTTPS endpoint
# ---------------------------------------------------------------------------

resource "aws_acm_certificate" "http" {
  domain_name       = var.domain_name
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "http_validation" {
  # Keyed by the hostname, which is known at plan time.
  for_each = toset([var.domain_name])

  zone_id = var.route53_zone_id
  name    = local.http_validation[each.key].resource_record_name
  type    = local.http_validation[each.key].resource_record_type
  records = [local.http_validation[each.key].resource_record_value]
  ttl     = 300
}

locals {
  http_validation = {
    for o in aws_acm_certificate.http.domain_validation_options : o.domain_name => o
  }
}

resource "aws_acm_certificate_validation" "http" {
  certificate_arn         = aws_acm_certificate.http.arn
  validation_record_fqdns = [for r in aws_route53_record.http_validation : r.fqdn]
}

resource "aws_route53_record" "http" {
  zone_id = var.route53_zone_id
  name    = var.domain_name
  type    = "A"

  alias {
    name                   = aws_lb.http.dns_name
    zone_id                = aws_lb.http.zone_id
    evaluate_target_health = true
  }
}

# ---------------------------------------------------------------------------
# ALB: Git over HTTPS
# ---------------------------------------------------------------------------

resource "aws_lb" "http" {
  name_prefix        = substr(var.name, 0, 6)
  load_balancer_type = "application"
  internal           = false
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.alb.id]

  idle_timeout               = var.alb_idle_timeout
  drop_invalid_header_fields = true
  enable_deletion_protection = var.load_balancer_deletion_protection

  tags = { Name = "${var.name}-http" }
}

resource "aws_lb_target_group" "http" {
  name_prefix = substr(var.name, 0, 6)
  target_type = "ip"
  vpc_id      = aws_vpc.this.id
  protocol    = "HTTP"
  port        = var.http_port

  # Give in-flight pushes and clones time to finish during deployments.
  deregistration_delay = 120

  health_check {
    path                = "/healthz"
    matcher             = "200"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_lb_listener" "http_redirect" {
  load_balancer_arn = aws_lb.http.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"

    redirect {
      protocol    = "HTTPS"
      port        = "443"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.http.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = aws_acm_certificate_validation.http.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.http.arn
  }
}

# ---------------------------------------------------------------------------
# NLB: Git over SSH (served from plan node C3 on; wired now)
# ---------------------------------------------------------------------------

resource "aws_lb" "ssh" {
  count = var.enable_ssh ? 1 : 0

  name_prefix        = substr(var.name, 0, 6)
  load_balancer_type = "network"
  internal           = false
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.nlb[0].id]

  enable_cross_zone_load_balancing = true
  enable_deletion_protection       = var.load_balancer_deletion_protection

  tags = { Name = "${var.name}-ssh" }
}

resource "aws_lb_target_group" "ssh" {
  count = var.enable_ssh ? 1 : 0

  name_prefix = substr(var.name, 0, 6)
  target_type = "ip"
  vpc_id      = aws_vpc.this.id
  protocol    = "TCP"
  port        = var.ssh_port

  # Aspen's SSH limits (unauthenticated connections and failed
  # authentications per address) key on the TCP peer, so tasks must see each
  # client's own address, not the NLB's (issue #120). Aspen does not parse
  # proxy protocol, so this is the only way to pass it. The tasks' security
  # group still admits only the NLB's security group: AWS matches that rule
  # with client IP preservation on. A task cannot reach itself through the
  # NLB (no hairpinning with preservation on); nothing in Aspen does.
  preserve_client_ip   = true
  proxy_protocol_v2    = false
  deregistration_delay = 120

  # ECS replaces tasks that fail any of their target groups' health checks, so
  # probe the HTTP health endpoint rather than the SSH port. That keeps the
  # service healthy before Aspen serves SSH.
  health_check {
    protocol            = "HTTP"
    port                = tostring(var.http_port)
    path                = "/healthz"
    matcher             = "200"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_lb_listener" "ssh" {
  count = var.enable_ssh ? 1 : 0

  load_balancer_arn = aws_lb.ssh[0].arn
  port              = var.ssh_public_port
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ssh[0].arn
  }
}

resource "aws_route53_record" "ssh" {
  count = var.enable_ssh ? 1 : 0

  zone_id = var.route53_zone_id
  name    = local.ssh_domain_name
  type    = "A"

  alias {
    name                   = aws_lb.ssh[0].dns_name
    zone_id                = aws_lb.ssh[0].zone_id
    evaluate_target_health = true
  }
}

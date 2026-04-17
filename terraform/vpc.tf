# VPC and networking configuration for feature store
resource "aws_vpc" "feature_store" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name        = "feature-store-vpc"
    Environment = var.environment
  }
}

resource "aws_internet_gateway" "feature_store" {
  vpc_id = aws_vpc.feature_store.id

  tags = {
    Name = "feature-store-igw"
  }
}

# Public subnets for ALB
resource "aws_subnet" "public" {
  count = 2

  vpc_id                  = aws_vpc.feature_store.id
  cidr_block              = "10.0.${count.index + 1}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "feature-store-public-${count.index + 1}"
    Type = "public"
  }
}

# Private subnets for ECS/RDS
resource "aws_subnet" "private" {
  count = 2

  vpc_id            = aws_vpc.feature_store.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "feature-store-private-${count.index + 1}"
    Type = "private"
  }
}

# NAT Gateway for private subnet internet access
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "feature-store-nat-eip"
  }
}

resource "aws_nat_gateway" "feature_store" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = "feature-store-nat"
  }

  depends_on = [aws_internet_gateway.feature_store]
}

# Route tables
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.feature_store.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.feature_store.id
  }

  tags = {
    Name = "feature-store-public-rt"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.feature_store.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.feature_store.id
  }

  tags = {
    Name = "feature-store-private-rt"
  }
}

# Route table associations
resource "aws_route_table_association" "public" {
  count = 2

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count = 2

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

data "aws_availability_zones" "available" {
  state = "available"
}
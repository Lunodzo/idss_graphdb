#!/usr/bin/env python3
"""
Generate deterministic energy-community graph data for an IDSS peer.

All rights reserved.
"""

import argparse
import hashlib
import json
import math
import random
import sys
from datetime import datetime, timedelta, timezone


def parse_arguments():
    parser = argparse.ArgumentParser(description="Generate energy-community graph data for IDSS")
    parser.add_argument("output_file", help="Path to the output JSON file")
    parser.add_argument("--num-customers", type=int, default=4, help="Number of Customer nodes")
    parser.add_argument("--days", type=int, default=1, help="Number of days of meter readings")
    parser.add_argument("--interval-minutes", type=int, default=15, help="Meter-reading interval in minutes")
    parser.add_argument("--generation-fraction", type=float, default=0.5, help="Fraction of customers with PV units")
    parser.add_argument("--battery-fraction", type=float, default=0.2, help="Fraction of customers with battery units")
    parser.add_argument("--seed", default="default", help="Peer-specific seed used for deterministic data")
    return parser.parse_args()


def edge(key, kind, end1_key, end1_kind, end1_role, end2_key, end2_kind, end2_role):
    return {
        "key": key,
        "kind": kind,
        "end1key": end1_key,
        "end1kind": end1_kind,
        "end1role": end1_role,
        "end1cascading": False,
        "end2key": end2_key,
        "end2kind": end2_kind,
        "end2role": end2_role,
        "end2cascading": True,
    }


def node(kind, key, **properties):
    return {"kind": kind, "key": key, "mRID": key, **properties}


def interval_timestamp(start, interval_index, interval_minutes):
    return (start + timedelta(minutes=interval_index * interval_minutes)).isoformat().replace("+00:00", "Z")


def consumption_profile(hour, random_source):
    morning_peak = 1.4 * math.exp(-((hour - 7.5) / 2.0) ** 2)
    evening_peak = 2.2 * math.exp(-((hour - 19.0) / 2.8) ** 2)
    return round(max(0.15, 0.35 + morning_peak + evening_peak + random_source.uniform(-0.08, 0.08)), 3)


def pv_profile(hour, rated_capacity, random_source):
    daylight = max(0.0, math.sin(math.pi * (hour - 6.0) / 12.0))
    return round(rated_capacity * daylight * random_source.uniform(0.78, 0.96), 3)


def main():
    args = parse_arguments()
    if args.num_customers < 1 or args.days < 1 or args.interval_minutes < 1:
        print("Error: customers, days, and interval minutes must be positive", file=sys.stderr)
        return 1
    if not 0 <= args.generation_fraction <= 1 or not 0 <= args.battery_fraction <= 1:
        print("Error: generation and battery fractions must be between 0 and 1", file=sys.stderr)
        return 1
    if 1440 % args.interval_minutes != 0:
        print("Error: interval minutes must divide 1440", file=sys.stderr)
        return 1

    seed = int(hashlib.sha256(args.seed.encode("utf-8")).hexdigest()[:16], 16)
    random_source = random.Random(seed)
    peer_prefix = hashlib.sha256(args.seed.encode("utf-8")).hexdigest()[:8]
    nodes = []
    edges = []
    customers = []
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    intervals_per_day = 1440 // args.interval_minutes

    manager_key = f"customer-{peer_prefix}-manager"
    nodes.append(node("Customer", manager_key, name="Community Manager", role="manager", membershipStatus="active", contractNumber=f"EC-{peer_prefix}-MGR"))

    for index in range(1, args.num_customers + 1):
        customer_key = f"customer-{peer_prefix}-{index:03d}"
        role = ("consumer", "prosumer", "producer")[index % 3]
        customer = node("Customer", customer_key, name=f"Community Member {index}", role=role, membershipStatus="active", contractNumber=f"EC-{peer_prefix}-{index:04d}")
        nodes.append(customer)
        customers.append(customer)
        edges.append(edge(f"member-{peer_prefix}-{index}", "memberOf", customer_key, "Customer", "member", manager_key, "Customer", "community"))

        usage_point_key = f"usage-point-{peer_prefix}-{index:03d}"
        device_key = f"end-device-{peer_prefix}-{index:03d}"
        nodes.append(node("UsagePoint", usage_point_key, connectionPoint=f"CP-{index:03d}", ratedPower=round(random_source.uniform(3.0, 11.0), 2), phase=random_source.choice(["single", "three"])))
        nodes.append(node("EndDevice", device_key, deviceType="meter", serial=f"MTR-{peer_prefix}-{index:04d}"))
        edges.append(edge(f"owns-point-{peer_prefix}-{index}", "owns", customer_key, "Customer", "owner", usage_point_key, "UsagePoint", "asset"))
        edges.append(edge(f"owns-device-{peer_prefix}-{index}", "owns", customer_key, "Customer", "owner", device_key, "EndDevice", "asset"))

        has_generation = index <= math.ceil(args.num_customers * args.generation_fraction)
        has_battery = index <= math.ceil(args.num_customers * args.battery_fraction)
        generating_unit_key = None
        battery_key = None
        if has_generation:
            generating_unit_key = f"generating-unit-{peer_prefix}-{index:03d}"
            rated_capacity = round(random_source.uniform(2.5, 8.0), 2)
            nodes.append(node("GeneratingUnit", generating_unit_key, unitType="PV", ratedCapacity=rated_capacity))
            edges.append(edge(f"owns-generation-{peer_prefix}-{index}", "owns", customer_key, "Customer", "owner", generating_unit_key, "GeneratingUnit", "asset"))
        if has_battery:
            battery_key = f"battery-unit-{peer_prefix}-{index:03d}"
            capacity = round(random_source.uniform(5.0, 13.5), 2)
            nodes.append(node("BatteryUnit", battery_key, ratedCapacity=capacity, maxChargePower=round(capacity / 2, 2), maxDischargePower=round(capacity / 2, 2)))
            edges.append(edge(f"owns-battery-{peer_prefix}-{index}", "owns", customer_key, "Customer", "owner", battery_key, "BatteryUnit", "asset"))

        state_of_charge = random_source.uniform(0.3, 0.7) * (capacity if has_battery else 1)
        for interval_index in range(args.days * intervals_per_day):
            timestamp = interval_timestamp(start, interval_index, args.interval_minutes)
            hour = (interval_index % intervals_per_day) * args.interval_minutes / 60
            active_reading_key = f"reading-{peer_prefix}-{index:03d}-{interval_index:05d}-load"
            nodes.append(node("MeterReading", active_reading_key, timeStamp=timestamp, value=consumption_profile(hour, random_source), readingType="activePower"))
            edges.append(edge(f"records-load-{peer_prefix}-{index}-{interval_index}", "records", usage_point_key, "UsagePoint", "point", active_reading_key, "MeterReading", "reading"))
            reactive_reading_key = f"reading-{peer_prefix}-{index:03d}-{interval_index:05d}-reactive"
            nodes.append(node("MeterReading", reactive_reading_key, timeStamp=timestamp, value=round(consumption_profile(hour, random_source) * random_source.uniform(0.15, 0.35), 3), readingType="reactivePower"))
            edges.append(edge(f"records-reactive-{peer_prefix}-{index}-{interval_index}", "records", device_key, "EndDevice", "point", reactive_reading_key, "MeterReading", "reading"))
            if generating_unit_key:
                generation_reading_key = f"reading-{peer_prefix}-{index:03d}-{interval_index:05d}-generation"
                nodes.append(node("MeterReading", generation_reading_key, timeStamp=timestamp, value=pv_profile(hour, rated_capacity, random_source), readingType="generation"))
                edges.append(edge(f"records-generation-{peer_prefix}-{index}-{interval_index}", "records", generating_unit_key, "GeneratingUnit", "point", generation_reading_key, "MeterReading", "reading"))
            if battery_key:
                state_of_charge = min(capacity, max(0.0, state_of_charge + random_source.uniform(-0.4, 0.4)))
                battery_reading_key = f"reading-{peer_prefix}-{index:03d}-{interval_index:05d}-soc"
                nodes.append(node("MeterReading", battery_reading_key, timeStamp=timestamp, value=round(state_of_charge, 3), readingType="stateOfCharge"))
                edges.append(edge(f"records-battery-{peer_prefix}-{index}-{interval_index}", "records", battery_key, "BatteryUnit", "point", battery_reading_key, "MeterReading", "reading"))

    for index, customer in enumerate(customers, start=1):
        offer_key = f"offer-{peer_prefix}-{index:03d}"
        bid_key = f"bid-{peer_prefix}-{index:03d}"
        valid_from = interval_timestamp(start, index % 4, args.interval_minutes)
        valid_to = interval_timestamp(start, (index % 4) + 4, args.interval_minutes)
        nodes.append(node("Offer", offer_key, quantity=round(random_source.uniform(1.0, 6.0), 3), price=round(random_source.uniform(0.15, 0.35), 3), validFrom=valid_from, validTo=valid_to, status="open"))
        nodes.append(node("Bid", bid_key, quantity=round(random_source.uniform(1.0, 6.0), 3), priceLimit=round(random_source.uniform(0.20, 0.40), 3), validFrom=valid_from, validTo=valid_to, status="open"))
        edges.append(edge(f"places-offer-{peer_prefix}-{index}", "places", customer["key"], "Customer", "actor", offer_key, "Offer", "order"))
        edges.append(edge(f"places-bid-{peer_prefix}-{index}", "places", customer["key"], "Customer", "actor", bid_key, "Bid", "order"))

    for index in range(1, max(2, args.num_customers // 2 + 1)):
        seller = customers[(index - 1) % len(customers)]
        buyer = customers[index % len(customers)]
        offer_key = f"offer-{peer_prefix}-concluded-{index:03d}"
        bid_key = f"bid-{peer_prefix}-concluded-{index:03d}"
        trade_key = f"trade-{peer_prefix}-{index:03d}"
        timestamp = interval_timestamp(start, index, args.interval_minutes)
        quantity = round(random_source.uniform(1.0, 4.0), 3)
        price = round(random_source.uniform(0.18, 0.32), 3)
        nodes.extend([
            node("Offer", offer_key, quantity=quantity, price=price, validFrom=timestamp, validTo=timestamp, status="matched"),
            node("Bid", bid_key, quantity=quantity, priceLimit=price, validFrom=timestamp, validTo=timestamp, status="matched"),
            node("Trade", trade_key, volume=quantity, price=price, timeStamp=timestamp, counterparty=buyer["mRID"], settlementRef=f"SET-{peer_prefix}-{index:03d}"),
        ])
        edges.extend([
            edge(f"places-concluded-offer-{peer_prefix}-{index}", "places", seller["key"], "Customer", "actor", offer_key, "Offer", "order"),
            edge(f"places-concluded-bid-{peer_prefix}-{index}", "places", buyer["key"], "Customer", "actor", bid_key, "Bid", "order"),
            edge(f"matches-offer-{peer_prefix}-{index}", "matches", trade_key, "Trade", "trade", offer_key, "Offer", "order"),
            edge(f"matches-bid-{peer_prefix}-{index}", "matches", trade_key, "Trade", "trade", bid_key, "Bid", "order"),
        ])

    with open(args.output_file, "w", encoding="utf-8") as output_file:
        json.dump({"nodes": nodes, "edges": edges}, output_file, separators=(",", ":"))
    print(f"Generated {len(nodes)} EC nodes and {len(edges)} EC edges using seed {args.seed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env bash
set -euo pipefail

duration="${1:-15}"
brokers="${KAFKA_BROKERS:?KAFKA_BROKERS is required}"
rules=()

cleanup() {
  for rule in "${rules[@]}"; do
    iptables -D OUTPUT -p tcp -d "${rule}" --dport 9092 -j DROP 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

IFS=',' read -r -a endpoints <<< "${brokers}"
for endpoint in "${endpoints[@]}"; do
  host="${endpoint%%:*}"
  while read -r address; do
    if [[ -n "${address}" ]]; then
      iptables -I OUTPUT -p tcp -d "${address}" --dport 9092 -j DROP
      rules+=("${address}")
    fi
  done < <(getent ahostsv4 "${host}" | awk '{print $1}' | sort -u)
done

if [[ "${#rules[@]}" -eq 0 ]]; then
  echo "no Kafka broker address resolved" >&2
  exit 1
fi

echo "dropping Kafka traffic to ${rules[*]} for ${duration}s"
sleep "${duration}"
echo "Kafka traffic restored"

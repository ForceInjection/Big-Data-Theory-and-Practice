#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

JAR_FILE=$(ls "${PROJECT_ROOT}"/target/gossip-lab-*.jar 2>/dev/null | head -n 1 || true)

if [ ! -f "${JAR_FILE}" ]; then
  echo -e "${YELLOW}[INFO] JAR not found, building gossip-lab...${NC}"
  cd "${PROJECT_ROOT}"
  bash build.sh
  JAR_FILE=$(ls "${PROJECT_ROOT}"/target/gossip-lab-*.jar 2>/dev/null | head -n 1 || true)
fi

if [ ! -f "${JAR_FILE}" ]; then
  echo -e "${RED}[ERROR] Build failed or JAR file not found in target/${NC}"
  exit 1
fi

NODE_COUNT="${1:-5}"
ROUNDS="${2:-20}"
FAILED_INDEX="${3:-2}"
FAILURE_ROUND="${4:-5}"
SUSPECT_THRESHOLD="${5:-3}"
DEAD_THRESHOLD="${6:-6}"

echo -e "${GREEN}[INFO] Using JAR: ${JAR_FILE}${NC}"
echo -e "${GREEN}[INFO] Node count: ${NODE_COUNT}, rounds: ${ROUNDS}, failedIndex: ${FAILED_INDEX}, failureRound: ${FAILURE_ROUND}, suspectThreshold: ${SUSPECT_THRESHOLD}, deadThreshold: ${DEAD_THRESHOLD}${NC}"

java -cp "${JAR_FILE}" com.bigdatatheory.gossip.membership.MembershipSimulationMain "${NODE_COUNT}" "${ROUNDS}" "${FAILED_INDEX}" "${FAILURE_ROUND}" "${SUSPECT_THRESHOLD}" "${DEAD_THRESHOLD}"

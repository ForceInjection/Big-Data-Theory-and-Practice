#!/bin/bash

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"
TARGET_DIR="${PROJECT_ROOT}/target"
MAVEN_IMAGE="maven:3.8.6-openjdk-11"

mkdir -p "${BUILD_DIR}/logs"
mkdir -p "${TARGET_DIR}"

LOG_FILE="${BUILD_DIR}/logs/gossip-lab-build-$(date +%Y%m%d-%H%M%S).log"

if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    docker run --rm \
      -v "${PROJECT_ROOT}":/workspace \
      -v "${HOME}/.m2":/root/.m2 \
      -w /workspace \
      "${MAVEN_IMAGE}" \
      mvn clean package -DskipTests=false \
      | tee "${LOG_FILE}"
    exit "${PIPESTATUS[0]}"
  fi
fi

mkdir -p "${TARGET_DIR}/classes"

find "${PROJECT_ROOT}/src/main/java" -name '*.java' > "${BUILD_DIR}/java-sources.list"

javac -d "${TARGET_DIR}/classes" @"${BUILD_DIR}/java-sources.list" | tee -a "${LOG_FILE}"

jar --create --file "${TARGET_DIR}/gossip-lab-1.0.0-SNAPSHOT.jar" -C "${TARGET_DIR}/classes" . | tee -a "${LOG_FILE}"

exit 0

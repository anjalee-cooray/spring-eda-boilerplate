#!/usr/bin/env bash
# Scaffold a new event schema version + upcaster stub.
#
# Usage:
#   ./scripts/new-schema.sh --event-type example.created --version 2 --service example-consumer-service
#
# Creates:
#   shared/shared-events/src/main/resources/schemas/<eventType>/v<N>.json
#   services/<service>/src/main/java/.../<EventType>V<N-1>ToV<N>Upcaster.java

set -euo pipefail

EVENT_TYPE=""
VERSION=""
SERVICE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --event-type) EVENT_TYPE="$2"; shift 2 ;;
    --version)    VERSION="$2";    shift 2 ;;
    --service)    SERVICE="$2";    shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [[ -z "$EVENT_TYPE" || -z "$VERSION" || -z "$SERVICE" ]]; then
  echo "Usage: $0 --event-type <type> --version <N> --service <service-module-name>"
  echo "  Example: $0 --event-type example.created --version 2 --service example-consumer-service"
  exit 1
fi

if ! [[ "$VERSION" =~ ^[0-9]+$ ]] || [[ "$VERSION" -lt 2 ]]; then
  echo "Error: --version must be an integer >= 2 (version 1 is the baseline)"
  exit 1
fi

PREV_VERSION=$((VERSION - 1))

# Derive class name components
# example.created → ExampleCreated
PASCAL=$(echo "$EVENT_TYPE" | sed 's/\.\([a-z]\)/\U\1/g; s/^\([a-z]\)/\U\1/g')
UPCASTER_CLASS="${PASCAL}V${PREV_VERSION}ToV${VERSION}Upcaster"

SCHEMA_DIR="shared/shared-events/src/main/resources/schemas/${EVENT_TYPE}"
SCHEMA_FILE="${SCHEMA_DIR}/v${VERSION}.json"

# Derive Java package from service name: example-consumer-service → com.example.eda.consumer.schema
BASE_PACKAGE="com.example.eda"
SERVICE_SEGMENT=$(echo "$SERVICE" | sed 's/example-//' | sed 's/-service//' | sed 's/-/./g')
PACKAGE="${BASE_PACKAGE}.${SERVICE_SEGMENT}.schema"

# Derive source directory
SERVICE_SRC="services/${SERVICE}/src/main/java/$(echo "$PACKAGE" | sed 's/\./\//g')"
UPCASTER_FILE="${SERVICE_SRC}/${UPCASTER_CLASS}.java"

echo "Creating schema file: ${SCHEMA_FILE}"
mkdir -p "${SCHEMA_DIR}"
cat > "${SCHEMA_FILE}" <<JSON
{
  "\$schema": "http://json-schema.org/draft-07/schema#",
  "title": "${EVENT_TYPE} v${VERSION}",
  "description": "Schema version ${VERSION} for ${EVENT_TYPE} events. Describe changes from v${PREV_VERSION} here.",
  "type": "object",
  "required": [],
  "properties": {
    "FILL_IN": {
      "type": "string",
      "description": "Add/modify fields here compared to v${PREV_VERSION}"
    }
  }
}
JSON

echo "Creating upcaster stub: ${UPCASTER_FILE}"
mkdir -p "${SERVICE_SRC}"
cat > "${UPCASTER_FILE}" <<JAVA
package ${PACKAGE};

import com.example.eda.events.schema.EventUpcaster;
import com.example.eda.events.schema.EventVersion;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

/**
 * Upcasts ${EVENT_TYPE} from schema v${PREV_VERSION} to v${VERSION}.
 *
 * Implement the upcast() method to migrate the payload:
 *   - Add new required fields with default values
 *   - Rename or split existing fields
 *   - Remove deprecated fields
 *
 * The registry applies upcasters in version order, so events arrive at this
 * upcaster already at v${PREV_VERSION} shape.
 */
@Component
@EventVersion(eventType = "${EVENT_TYPE}", fromVersion = "${PREV_VERSION}", toVersion = "${VERSION}")
public class ${UPCASTER_CLASS} implements EventUpcaster {

    @Override
    public ObjectNode upcast(ObjectNode payload) {
        // TODO: migrate v${PREV_VERSION} → v${VERSION} payload
        // Example: payload.put("newField", "defaultValue");
        //          payload.remove("deprecatedField");
        return payload;
    }
}
JAVA

echo ""
echo "✓ Schema stub created:   ${SCHEMA_FILE}"
echo "✓ Upcaster stub created: ${UPCASTER_FILE}"
echo ""
echo "Next steps:"
echo "  1. Fill in the JSON schema fields in ${SCHEMA_FILE}"
echo "  2. Implement ${UPCASTER_CLASS}.upcast() to migrate v${PREV_VERSION} payloads"
echo "  3. Add tests in ${SERVICE_SRC}/.../${UPCASTER_CLASS}Test.java"
echo "  4. Update EventSchemaRegistry if manually registering schemas"

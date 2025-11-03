# DetectorAaveGov Configuration

## Overview

DetectorAaveGov monitors Aave Governance V3 proposals and generates alerts based on proposal state and voting conditions.

## Configuration Options

### Basic Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_key` | string | `ee1eec9e5b0dc51fef435de760a14269` | The Graph API key |
| `proposal_count` | int | `5` | Number of recent proposals to check (when not using specific IDs) |
| `proposal_ids` | string | `""` | Comma-separated list of specific proposal IDs to monitor (e.g., "400,399,398") |
| `desc` | string | `"{state}: {title}"` | Description template with placeholders. Available: `{state}`, `{title}`, `{proposal_id}`, `{creator}`, `{vote_yes}`, `{vote_no}`, etc. |

### Tracking Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `track_active` | boolean | `true` | Monitor Active proposals (state=2) |
| `track_cancelled` | boolean | `true` | Monitor Cancelled proposals (state=6) |
| `track_executed` | boolean | `true` | Monitor Executed proposals (state=4) |

## Alert Logic

### Active Proposals (state=2)
- **Severity**: INFO
- **Condition**: Always alert when tracked
- **Reason**: "Proposal ACTIVE - voting in progress"

### Cancelled Proposals (state=6)
- **Severity**: HIGH
- **Condition**: Always alert when tracked
- **Reason**: "Proposal CANCELLED"

### Executed Proposals (state=4)
- **Severity**: HIGH if:
  - Quorum NOT reached, OR
  - NAY votes > YAE votes (votes against > votes for)
- **Severity**: INFO if executed successfully with quorum met and YAE > NAY
- **Reasons**:
  - "Proposal EXECUTED but quorum NOT reached"
  - "Proposal EXECUTED but NAY > YAE"
  - "Proposal EXECUTED successfully"

## Configuration Examples

### Example 1: Monitor Last 5 Proposals (All States)

```hocon
detector {
  name = "aave-gov"
  plugin = "detector-aave-gov"

  data {
    proposal_count = 5
    track_active = true
    track_cancelled = true
    track_executed = true
  }
}
```

### Example 2: Monitor Specific Proposals Only

```hocon
detector {
  name = "aave-gov-specific"
  plugin = "detector-aave-gov"

  data {
    proposal_ids = "400,399,398,371"
    track_active = true
    track_cancelled = true
    track_executed = true
  }
}
```

### Example 3: Monitor Only Active and Cancelled

```hocon
detector {
  name = "aave-gov-active-cancelled"
  plugin = "detector-aave-gov"

  data {
    proposal_count = 10
    track_active = true
    track_cancelled = true
    track_executed = false
  }
}
```

### Example 4: Monitor Specific Cancelled Proposal

```hocon
detector {
  name = "aave-gov-371"
  plugin = "detector-aave-gov"

  data {
    proposal_ids = "371"
    track_active = false
    track_cancelled = true
    track_executed = false
  }
}
```

### Example 5: Custom Description Template

```hocon
detector {
  name = "aave-gov-custom-desc"
  plugin = "detector-aave-gov"

  data {
    proposal_count = 5
    desc = "Proposal #{proposal_id} [{state}]: {title} - YES: {vote_yes} AAVE"
  }
}
```

Result: `"desc": "Proposal #400 [Active]: Addition of cbBTC/Stablecoin E-Mode... - YES: 595938.60 AAVE"`

### Example 6: Minimal Description

```hocon
detector {
  name = "aave-gov-minimal"
  plugin = "detector-aave-gov"

  data {
    proposal_count = 5
    desc = "{reason}"
  }
}
```

Result: `"desc": "Proposal ACTIVE - voting in progress"`

## Description Template

The `desc` configuration parameter supports template placeholders that are replaced with actual values from the event metadata.

### Available Placeholders

| Placeholder | Description | Example Value |
|------------|-------------|---------------|
| `{proposal_id}` | Proposal ID | `"400"` |
| `{title}` | Proposal title | `"Addition of cbBTC/Stablecoin E-Mode..."` |
| `{creator}` | Creator address | `"0x57ab7ee15ce5ecacb1ab84ee42d5a9d0d8112922"` |
| `{state}` | Proposal state | `"Active"`, `"Cancelled"`, `"Executed"` |
| `{state_code}` | State code number | `"2"` |
| `{vote_yes}` | YES votes | `"595938.60"` |
| `{vote_no}` | NO votes | `"0.00"` |
| `{vote_total}` | Total votes | `"595938.60"` |
| `{vote_yes_percentage}` | YES percentage | `"100.00"` |
| `{vote_differential}` | Vote differential (YES - NO) | `"595938.60"` |
| `{vote_quorum}` | Required quorum | `"0.00"` |
| `{vote_quorum_met}` | Quorum met status | `"true"` or `"false"` |
| `{vote_count}` | Number of individual votes | `"23"` |
| `{vote_source}` | Source of vote data | `"individual votes (not bridged)"` |
| `{vote_chains}` | Chains with votes | `"Avalanche"` |
| `{reason}` | Alert reason | `"Proposal ACTIVE - voting in progress"` |

### Template Examples

```
Default:           "{state}: {title}"
Result:            "Active: Addition of cbBTC/Stablecoin E-Mode..."

Detailed:          "#{proposal_id} [{state}] {title} - {vote_yes}/{vote_no} AAVE"
Result:            "#400 [Active] Addition of cbBTC/Stablecoin E-Mode... - 595938.60/0.00 AAVE"

Reason only:       "{reason}"
Result:            "Proposal ACTIVE - voting in progress"

Custom:            "Aave Gov {state}: {vote_yes} YES vs {vote_no} NO"
Result:            "Aave Gov Active: 595938.60 YES vs 0.00 NO"
```

## Event ID (eid) Generation

Event IDs are generated deterministically based on proposal state:

### Cancelled and Executed Proposals
- **eid**: `{did}-{proposalId}-{extId}`
- **Purpose**: Deterministic ID ensures the same proposal always generates the same event
- **Example**: `detector-aave-gov-371-aave-gov-monitor`

### Active Proposals
- **eid**: `{did}-{proposalId}-{extId}-{votesFor}-{votesAgainst}`
- **Purpose**: ID changes when votes change, creating new events as voting progresses
- **Example**: `detector-aave-gov-400-aave-gov-monitor-595938-0`

### All Proposals
- **tx_hash**: Always set to `{proposalId}` for easy correlation

## Event Metadata

Each alert includes the following metadata (all vote-related attributes use `vote_` prefix):

```json
{
  "desc": "Active: Addition of cbBTC/Stablecoin E-Mode...",
  "proposal_id": "400",
  "title": "Addition of cbBTC/Stablecoin E-Mode...",
  "creator": "0x57ab7ee15ce5ecacb1ab84ee42d5a9d0d8112922",
  "state": "Active",
  "state_code": "2",
  "vote_yes": "595938.60",
  "vote_no": "0.00",
  "vote_total": "595938.60",
  "vote_yes_percentage": "100.00",
  "vote_differential": "595938.60",
  "vote_quorum": "0.00",
  "vote_quorum_met": "true",
  "vote_count": "23",
  "vote_source": "individual votes (not bridged)",
  "vote_chains": "Avalanche",
  "reason": "Proposal ACTIVE - voting in progress",
  "tx_hash": "400"
}
```

**Note**: The `desc` field is populated from the template (default: `"{state}: {title}"`). Placeholders like `{state}` and `{title}` are replaced with actual values.

## Rate Limiting

Default cron rate limits by environment:
- **Test**: 10 seconds
- **Dev**: 1 minute
- **Production**: 10 minutes

## Notes

1. When `proposal_ids` is specified, `proposal_count` is ignored
2. Proposals are only processed if their tracking flag is enabled
3. Vote data is fetched from all three chains: Ethereum, Polygon, Avalanche
4. Individual votes are used when aggregate is 0 (not yet bridged)
5. All proposals with state other than Active, Cancelled, or Executed are skipped

## Event Deduplication Behavior

The event ID (eid) strategy ensures proper deduplication:

- **Cancelled/Executed**: Always generate the **same** eid, so multiple detector runs produce only one alert per proposal
- **Active**: Generate **different** eids as votes change, creating new alerts to track voting progress
  - Example: First run generates `detector-aave-gov-400-aave-gov-monitor-595938-0`
  - Next run with more votes generates `detector-aave-gov-400-aave-gov-monitor-610000-100`
  - This allows tracking vote progression over time

This pattern prevents alert spam for static states while enabling real-time tracking of active voting.

### Example Event IDs:

```
Proposal 371 (Cancelled):
  Run 1: eid = "detector-aave-gov-371-aave-gov-monitor" ✓ (new alert)
  Run 2: eid = "detector-aave-gov-371-aave-gov-monitor" ✗ (duplicate, filtered out)

Proposal 400 (Active):
  Run 1: eid = "detector-aave-gov-400-aave-gov-monitor-595938-0" ✓ (new alert)
  Run 2: eid = "detector-aave-gov-400-aave-gov-monitor-610000-100" ✓ (votes changed, new alert)
  Run 3: eid = "detector-aave-gov-400-aave-gov-monitor-610000-100" ✗ (same votes, duplicate)
```

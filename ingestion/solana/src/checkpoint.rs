//! Checkpoint math. A high-water slot is durable coverage, not a guess.

/// Highest slot that is fully resolved inside the requested window.
///
/// Enumeration must finish first. Unfetched older pages are unresolved gaps,
/// so a complete newest page does not move the checkpoint.
pub fn high_water_slot(
    from_slot: i64,
    to_slot: i64,
    enumeration_complete: bool,
    earliest_pending_slot: Option<i64>,
) -> Option<i64> {
    if !enumeration_complete {
        return None;
    }
    match earliest_pending_slot {
        None => Some(to_slot),
        Some(slot) if slot <= from_slot => None,
        Some(slot) => Some(slot - 1),
    }
}

pub fn record_key(
    source_id: &str,
    cluster_id: &str,
    signature: &str,
    record_identity: &str,
    decoder_version: &str,
) -> String {
    format!("{source_id}\u{1f}{cluster_id}\u{1f}{signature}\u{1f}{record_identity}\u{1f}{decoder_version}")
}

#[cfg(test)]
mod tests {
    use super::{high_water_slot, record_key};

    #[test]
    fn incomplete_enumeration_does_not_advance() {
        assert_eq!(high_water_slot(10, 20, false, None), None);
        assert_eq!(high_water_slot(10, 20, false, Some(15)), None);
    }

    #[test]
    fn pending_at_the_window_start_blocks_advancement() {
        assert_eq!(high_water_slot(10, 20, true, Some(10)), None);
    }

    #[test]
    fn high_water_stops_before_the_oldest_pending_slot() {
        assert_eq!(high_water_slot(10, 20, true, Some(15)), Some(14));
    }

    #[test]
    fn a_fully_resolved_window_reaches_the_upper_bound() {
        assert_eq!(high_water_slot(10, 20, true, None), Some(20));
    }

    #[test]
    fn identity_changes_when_the_decoder_changes() {
        let first = record_key(
            "solana-mainnet",
            "mainnet-beta",
            "sig",
            "transaction",
            "generic-v1",
        );
        let second = record_key(
            "solana-mainnet",
            "mainnet-beta",
            "sig",
            "transaction",
            "generic-v2",
        );
        assert_ne!(first, second);
        assert_eq!(
            first,
            record_key(
                "solana-mainnet",
                "mainnet-beta",
                "sig",
                "transaction",
                "generic-v1"
            )
        );
    }
}

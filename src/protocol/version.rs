//! Protocol versions and their negotiation.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A protocol version. Ordered, so "highest common" is a plain `min`.
///
/// A new *minor* adds messages or optional fields an old peer can ignore or
/// refuse explicitly; a new *major* may change the meaning of existing ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProtocolVersion {
    /// Incompatible changes.
    pub major: u16,
    /// Backwards-compatible additions.
    pub minor: u16,
}

impl ProtocolVersion {
    /// The version this build speaks.
    pub const CURRENT: Self = Self::new(1, 0);

    /// Builds a version.
    #[must_use]
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// The versions a node can speak, inclusive on both ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRange {
    /// The oldest version still spoken.
    pub min: ProtocolVersion,
    /// The newest version spoken.
    pub max: ProtocolVersion,
}

impl VersionRange {
    /// Exactly the version this build speaks.
    pub const CURRENT: Self = Self {
        min: ProtocolVersion::CURRENT,
        max: ProtocolVersion::CURRENT,
    };

    /// Picks the version both sides will use: the highest both support.
    ///
    /// `None` means the ranges do not overlap and the session must be refused;
    /// there is no silent downgrade past what either side declared.
    #[must_use]
    pub fn negotiate(&self, other: &Self) -> Option<ProtocolVersion> {
        let highest = self.max.min(other.max);
        (highest >= self.min && highest >= other.min).then_some(highest)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn range(min: (u16, u16), max: (u16, u16)) -> VersionRange {
        VersionRange {
            min: ProtocolVersion::new(min.0, min.1),
            max: ProtocolVersion::new(max.0, max.1),
        }
    }

    #[rstest]
    #[case(range((1, 0), (1, 0)), range((1, 0), (1, 0)), Some((1, 0)))]
    #[case(range((1, 0), (1, 3)), range((1, 0), (1, 1)), Some((1, 1)))]
    #[case(range((1, 0), (2, 0)), range((1, 0), (1, 5)), Some((1, 5)))]
    #[case(range((1, 0), (1, 0)), range((2, 0), (2, 0)), None)]
    #[case(range((1, 2), (1, 4)), range((1, 0), (1, 1)), None)]
    fn both_sides_settle_on_the_highest_version_they_share(
        #[case] a: VersionRange,
        #[case] b: VersionRange,
        #[case] expected: Option<(u16, u16)>,
    ) {
        let expected = expected.map(|(major, minor)| ProtocolVersion::new(major, minor));
        assert_eq!(a.negotiate(&b), expected);
        assert_eq!(b.negotiate(&a), expected, "negotiation must be symmetric");
    }
}

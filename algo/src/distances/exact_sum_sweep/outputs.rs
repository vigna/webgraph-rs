/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */

/// The result returned by [`compute`](super::OutputLevel::run) on
/// [`All`](super::All).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct All {
    /// The forward eccentricities
    pub forward_eccentricities: Box<[usize]>,
    /// The backward eccentricities
    pub backward_eccentricities: Box<[usize]>,
    /// The diameter.
    pub diameter: usize,
    /// The radius.
    pub radius: usize,
    /// A vertex whose eccentricity equals the diameter.
    pub diametral_vertex: usize,
    /// A vertex whose eccentricity equals the radius.
    pub radial_vertex: usize,
    /// Number of iterations before the radius was found.
    pub radius_iterations: usize,
    /// Number of iterations before the diameter was found.
    pub diameter_iterations: usize,
    /// Number of iterations before all forward eccentricities were found.
    pub forward_iterations: usize,
    /// Number of iterations before all eccentricities were found.
    pub all_iterations: usize,
}

/// The result returned by [`compute`](super::OutputLevel::run) on
/// [`AllForward`](super::AllForward).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct AllForward {
    /// The forward eccentricities
    pub forward_eccentricities: Box<[usize]>,
    /// The diameter.
    pub diameter: usize,
    /// The radius.
    pub radius: usize,
    /// A vertex whose eccentricity equals the diameter.
    pub diametral_vertex: usize,
    /// A vertex whose eccentricity equals the radius.
    pub radial_vertex: usize,
    /// Number of iterations before the radius was found.
    pub radius_iterations: usize,
    /// Number of iterations before the diameter was found.
    pub diameter_iterations: usize,
    /// Number of iterations before all forward eccentricities are found.
    pub forward_iterations: usize,
}

/// The result returned by [`compute`](super::OutputLevel::run) on
/// [`RadiusDiameter`](super::RadiusDiameter).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RadiusDiameter {
    /// The diameter.
    pub diameter: usize,
    /// The radius.
    pub radius: usize,
    /// A vertex whose eccentricity equals the diameter.
    pub diametral_vertex: usize,
    /// A vertex whose eccentricity equals the radius.
    pub radial_vertex: usize,
    /// Number of iterations before the radius was found.
    pub radius_iterations: usize,
    /// Number of iterations before the diameter was found.
    pub diameter_iterations: usize,
}

/// The result returned by [`compute`](super::OutputLevel::run) on
/// [`Diameter`](super::Diameter).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Diameter {
    /// The diameter.
    pub diameter: usize,
    /// A vertex whose eccentricity equals the diameter.
    pub diametral_vertex: usize,
    /// Number of iterations before the diameter was found.
    pub diameter_iterations: usize,
}

/// The result returned by [`compute`](super::OutputLevel::run) on
/// [`Radius`](super::Radius).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Radius {
    /// The radius.
    pub radius: usize,
    /// A vertex whose eccentricity equals the radius.
    pub radial_vertex: usize,
    /// Number of iterations before the radius was found.
    pub radius_iterations: usize,
}

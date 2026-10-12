/*
 * SPDX-FileCopyrightText: 2024 Matteo Dell'Acqua
 * SPDX-FileCopyrightText: 2025 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

/// The result returned by [`run_symm`] on [`All`] or [`AllForward`].
///
/// [`run_symm`]: super::Level::run_symm
/// [`All`]: super::All
/// [`AllForward`]: super::AllForward
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct All {
    /// The eccentricities
    pub eccentricities: Box<[usize]>,
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
    /// Number of iterations before all eccentricities were found.
    pub iterations: usize,
}

// The results of the other levels are the same as in the directed case.
pub use super::output::{Diameter, Radius, RadiusDiameter};

use crate::models::{CellStats, ConfidenceLevel, FirstPassageCell, FirstPassageMatrix};

/// Z-score for 95% confidence interval
const Z_95: f64 = 1.96;

/// Calculate Wilson Score Confidence Interval for a binomial proportion
///
/// Returns (lower_bound, upper_bound) for P(success) at 95% confidence
pub fn wilson_score_interval(successes: u32, total: u32) -> (f64, f64) {
    if total == 0 {
        return (0.0, 1.0);
    }

    let n = total as f64;
    let p_hat = successes as f64 / n;
    let z = Z_95;
    let z_squared = z * z;

    let denominator = 1.0 + z_squared / n;

    let center = (p_hat + z_squared / (2.0 * n)) / denominator;

    let margin = z * ((p_hat * (1.0 - p_hat) + z_squared / (4.0 * n)) / n).sqrt() / denominator;

    let lower = (center - margin).max(0.0);
    let upper = (center + margin).min(1.0);

    (lower, upper)
}

/// Bayesian Beta-Binomial posterior
///
/// Prior: Beta(alpha_prior, beta_prior) - default is Beta(1, 1) = uniform
/// Posterior: Beta(alpha_prior + successes, beta_prior + failures)
///
/// Returns (posterior_alpha, posterior_beta)
pub fn beta_posterior(
    successes: u32,
    failures: u32,
    alpha_prior: f64,
    beta_prior: f64,
) -> (f64, f64) {
    let alpha = alpha_prior + successes as f64;
    let beta = beta_prior + failures as f64;
    (alpha, beta)
}

/// Calculate the mean of a Beta distribution
pub fn beta_mean(alpha: f64, beta: f64) -> f64 {
    alpha / (alpha + beta)
}

/// Calculate the mode of a Beta distribution (most likely value)
/// Only defined when alpha > 1 and beta > 1
pub fn beta_mode(alpha: f64, beta: f64) -> Option<f64> {
    if alpha > 1.0 && beta > 1.0 {
        Some((alpha - 1.0) / (alpha + beta - 2.0))
    } else {
        None
    }
}

/// Calculate credible interval for Beta distribution using quantiles
/// Returns (lower, upper) for the given credible level (e.g., 0.95 for 95%)
pub fn beta_credible_interval(alpha: f64, beta: f64, credible_level: f64) -> (f64, f64) {
    use statrs::distribution::{Beta, ContinuousCDF};

    let tail = (1.0 - credible_level) / 2.0;

    match Beta::new(alpha, beta) {
        Ok(dist) => {
            let lower = dist.inverse_cdf(tail);
            let upper = dist.inverse_cdf(1.0 - tail);
            (lower, upper)
        }
        Err(_) => (0.0, 1.0), // Fallback for invalid parameters
    }
}

/// Update a CellStats with calculated statistics
pub fn compute_cell_stats(cell: &mut CellStats) {
    let total = cell.total();

    if total == 0 {
        cell.p_up = 0.5;
        cell.p_down = 0.5;
        cell.p_up_wilson_lower = 0.0;
        cell.p_up_wilson_upper = 1.0;
        cell.beta_alpha = 1.0;
        cell.beta_beta = 1.0;
        cell.confidence_level = ConfidenceLevel::Unreliable;
        return;
    }

    // Raw probabilities
    cell.p_up = cell.count_up as f64 / total as f64;
    cell.p_down = cell.count_down as f64 / total as f64;

    // Wilson Score CI
    let (lower, upper) = wilson_score_interval(cell.count_up, total);
    cell.p_up_wilson_lower = lower;
    cell.p_up_wilson_upper = upper;

    // Bayesian posterior (uniform prior)
    let (alpha, beta) = beta_posterior(cell.count_up, cell.count_down, 1.0, 1.0);
    cell.beta_alpha = alpha;
    cell.beta_beta = beta;

    // Confidence level
    cell.confidence_level = ConfidenceLevel::from_sample_count(total);
}

// ============================================================================
// FIRST-PASSAGE MATRIX STATISTICS
// ============================================================================

/// Update a FirstPassageCell with calculated statistics
pub fn compute_first_passage_cell_stats(cell: &mut FirstPassageCell) {
    if cell.count_total == 0 {
        cell.p_reach = 0.0;
        cell.p_reach_wilson_lower = 0.0;
        cell.p_reach_wilson_upper = 1.0;
        cell.confidence_level = ConfidenceLevel::Unreliable;
        return;
    }

    // Raw probability
    cell.p_reach = cell.count_reached as f64 / cell.count_total as f64;

    // Wilson Score CI
    let (lower, upper) = wilson_score_interval(cell.count_reached, cell.count_total);
    cell.p_reach_wilson_lower = lower;
    cell.p_reach_wilson_upper = upper;

    // Confidence level
    cell.confidence_level = ConfidenceLevel::from_sample_count(cell.count_total);
}

/// Compute statistics for all cells in the first-passage matrix
pub fn compute_first_passage_matrix_stats(matrix: &mut FirstPassageMatrix) {
    for time_bucket in 0u8..60 {
        for delta_bucket in -17i8..=16i8 {
            let state = matrix.get_mut(time_bucket, delta_bucket);

            // Compute stats for all UP targets
            for target in -17i8..=16i8 {
                let cell = state.get_up_target_mut(target);
                compute_first_passage_cell_stats(cell);
            }

            // Compute stats for all DOWN targets
            for target in -17i8..=16i8 {
                let cell = state.get_down_target_mut(target);
                compute_first_passage_cell_stats(cell);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wilson_score_interval() {
        // 50 out of 100 successes
        let (lower, upper) = wilson_score_interval(50, 100);
        assert!(lower > 0.39 && lower < 0.41);
        assert!(upper > 0.59 && upper < 0.61);

        // 8 out of 10 successes (small sample)
        let (lower, upper) = wilson_score_interval(8, 10);
        assert!(lower > 0.44 && lower < 0.55);
        assert!(upper > 0.92 && upper < 0.99);

        // Edge case: 0 successes
        let (lower, upper) = wilson_score_interval(0, 10);
        assert!(lower >= 0.0);
        assert!(upper > 0.0 && upper < 0.4);

        // Edge case: all successes
        let (lower, upper) = wilson_score_interval(10, 10);
        assert!(lower > 0.6);
        assert!(upper <= 1.0);
    }

    #[test]
    fn test_beta_posterior() {
        // Uniform prior, 8 successes, 2 failures
        let (alpha, beta) = beta_posterior(8, 2, 1.0, 1.0);
        assert_eq!(alpha, 9.0);
        assert_eq!(beta, 3.0);

        let mean = beta_mean(alpha, beta);
        assert!((mean - 0.75).abs() < 0.01);
    }

    #[test]
    fn test_compute_cell_stats() {
        let mut cell = CellStats::new(0, 0);
        cell.count_up = 65;
        cell.count_down = 35;

        compute_cell_stats(&mut cell);

        assert!((cell.p_up - 0.65).abs() < 0.01);
        assert!((cell.p_down - 0.35).abs() < 0.01);
        assert!(cell.p_up_wilson_lower > 0.54);
        assert!(cell.p_up_wilson_upper < 0.75);
        assert_eq!(cell.confidence_level, ConfidenceLevel::Moderate);
    }
}

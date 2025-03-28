/*
 * SPDX-FileCopyrightText: 2023 Tommaso Fontana
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 */
use anyhow::Result;
use jiff::fmt::friendly::{Designator, Spacing, SpanPrinter};
use jiff::SpanRound;
use std::io::Write;
use std::time::SystemTime;
use webgraph::cli::main as cli_main;

pub fn main() -> Result<()> {
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));

    let start = std::time::Instant::now();
    let printer = SpanPrinter::new()
        .spacing(Spacing::None)
        .designator(Designator::Compact);
    let span_round = SpanRound::new()
        .largest(jiff::Unit::Day)
        .smallest(jiff::Unit::Millisecond)
        .days_are_24_hours();

    builder.format(move |buf, record| {
        let Ok(ts) = jiff::Timestamp::try_from(SystemTime::now()) else {
            return Err(std::io::Error::other("Failed to get timestamp"));
        };
        let style = buf.default_level_style(record.level());
        let elapsed = start.elapsed();
        let span = jiff::Span::new()
            .seconds(elapsed.as_secs() as i64)
            .milliseconds(elapsed.subsec_millis() as i64);
        let span = span.round(span_round).expect("Failed to round span");
        writeln!(
            buf,
            "{} {} {style}{}{style:#} [{:?}] {} - {}",
            ts.strftime("%F %T%.3f"),
            printer.span_to_string(&span),
            record.level(),
            std::thread::current().id(),
            record.target(),
            record.args()
        )
    });
    builder.init();

    // Call the main function of the CLI with cli args
    cli_main(std::env::args_os())
}

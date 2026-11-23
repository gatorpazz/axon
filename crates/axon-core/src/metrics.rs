//! A deliberately small counter registry rendered in the Prometheus text format.

use std::{collections::BTreeMap, fmt::Write, sync::Mutex};

type Key = (&'static str, Vec<(&'static str, String)>);

#[derive(Debug, Default)]
pub struct Metrics {
    counters: Mutex<BTreeMap<Key, u64>>,
}

impl Metrics {
    pub fn inc(&self, name: &'static str, labels: &[(&'static str, &str)]) {
        self.add(name, labels, 1);
    }

    /// Label values must come from a bounded set (never raw user input).
    pub fn add(&self, name: &'static str, labels: &[(&'static str, &str)], n: u64) {
        let labels = labels.iter().map(|&(k, v)| (k, v.to_owned())).collect();
        // A poisoned lock only means another thread panicked mid-increment;
        // the map itself is still valid.
        let mut counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        *counters.entry((name, labels)).or_default() += n;
    }

    pub fn get(&self, name: &'static str, labels: &[(&'static str, &str)]) -> u64 {
        let labels = labels.iter().map(|&(k, v)| (k, v.to_owned())).collect();
        let counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        counters.get(&(name, labels)).copied().unwrap_or(0)
    }

    pub fn render(&self) -> String {
        let counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = String::new();
        let mut current = "";
        for ((name, labels), value) in counters.iter() {
            if *name != current {
                current = name;
                let _ = writeln!(out, "# TYPE {name} counter");
            }
            out.push_str(name);
            if !labels.is_empty() {
                let rendered: Vec<_> = labels
                    .iter()
                    .map(|(k, v)| {
                        format!("{k}=\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""))
                    })
                    .collect();
                let _ = write!(out, "{{{}}}", rendered.join(","));
            }
            let _ = writeln!(out, " {value}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_grouped_counters() {
        let m = Metrics::default();
        m.inc(
            "axon_tool_invocations_total",
            &[("tool", "a"), ("outcome", "ok")],
        );
        m.inc(
            "axon_tool_invocations_total",
            &[("tool", "a"), ("outcome", "ok")],
        );
        m.inc(
            "axon_tool_invocations_total",
            &[("tool", "b"), ("outcome", "error")],
        );
        m.inc("axon_jobs_submitted_total", &[]);
        assert_eq!(
            m.render(),
            "# TYPE axon_jobs_submitted_total counter\n\
             axon_jobs_submitted_total 1\n\
             # TYPE axon_tool_invocations_total counter\n\
             axon_tool_invocations_total{tool=\"a\",outcome=\"ok\"} 2\n\
             axon_tool_invocations_total{tool=\"b\",outcome=\"error\"} 1\n"
        );
    }
}

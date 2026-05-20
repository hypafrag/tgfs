//! Throwaway probe: see what coaxes hunch into recognizing `TS-19` as an
//! episode title. Delete once we've picked a strategy.

fn dump(label: &str, input: &str) {
    let r = hunch::hunch(input);
    println!("{label:50} → title={:?}  episode_title={:?}",
        r.title().unwrap_or("-"),
        r.episode_title());
}

fn main() {
    let cases = [
        "as-is",                   "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06_TS-19.avi",
        "dash → dot",              "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06_TS.19.avi",
        "dash → space",            "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06_TS 19.avi",
        "dash → underscore",       "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06_TS_19.avi",
        "_ → space, dash kept",    "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06 TS-19.avi",
        "_ before title → dash",   "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06-TS-19.avi",
        "no dir",                  "s01e06_TS-19.avi",
        "ep01 baseline (works)",   "The.Walking.Dead.bdrip_[teko]/Season_01/s01e01_Days.Gone.Bye.avi",
        "TS-19 surrounded by dots","The.Walking.Dead.S01E06.TS-19.avi",
        "TS19 (no dash)",          "The.Walking.Dead.S01E06.TS19.avi",
        "dotted title kept dash",  "The.Walking.Dead.S01E06.TS-19.bdrip.avi",
        "as-is with siblings",     "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06_TS-19.avi",
    ];

    let mut i = 0;
    while i + 1 < cases.len() {
        dump(cases[i], cases[i + 1]);
        i += 2;
    }

    println!();
    println!("With hunch_with_context (siblings from the same season):");
    let target = "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06_TS-19.avi";
    let siblings = [
        "The.Walking.Dead.bdrip_[teko]/Season_01/s01e01_Days.Gone.Bye.avi",
        "The.Walking.Dead.bdrip_[teko]/Season_01/s01e02_Guts.avi",
        "The.Walking.Dead.bdrip_[teko]/Season_01/s01e03_Tell.It.to.the.Frogs.avi",
        "The.Walking.Dead.bdrip_[teko]/Season_01/s01e04_Vatos.avi",
        "The.Walking.Dead.bdrip_[teko]/Season_01/s01e05_Wildfire.avi",
    ];
    let r = hunch::hunch_with_context(target, &siblings);
    println!("  title={:?}  season={:?}  episode={:?}  episode_title={:?}",
        r.title(), r.season(), r.episode(), r.episode_title());
}

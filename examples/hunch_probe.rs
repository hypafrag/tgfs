//! Throwaway probe: feed example relative paths to hunch and dump every
//! field we care about so we can see what the parser can and can't extract
//! from the full-path form. Delete once the tvshow recursive-mode design
//! lands.

fn dump(label: &str, input: &str) {
    let r = hunch::hunch(input);
    println!("== {label} ==");
    println!("  input         : {input}");
    println!("  title         : {:?}", r.title());
    println!("  season        : {:?}", r.season());
    println!("  episode       : {:?}", r.episode());
    println!("  episode_title : {:?}", r.episode_title());
    println!("  year          : {:?}", r.year());
    println!("  is_episode    : {}", r.is_episode());
    println!("  confidence    : {:?}", r.confidence());
    println!();
}

fn main() {
    let paths = [
        ("full path",       "The.Walking.Dead.bdrip_[teko]/Season_01/s01e01_Days.Gone.Bye.avi"),
        ("filename only",   "s01e01_Days.Gone.Bye.avi"),
        ("path no group",   "The.Walking.Dead/Season_01/s01e01_Days.Gone.Bye.avi"),
        ("flattened",       "The.Walking.Dead.Season_01.s01e01_Days.Gone.Bye.avi"),
        ("ep02",            "The.Walking.Dead.bdrip_[teko]/Season_01/s01e02_Guts.avi"),
        ("ep05",            "The.Walking.Dead.bdrip_[teko]/Season_01/s01e05_Wildfire.avi"),
        ("ep06",            "The.Walking.Dead.bdrip_[teko]/Season_01/s01e06_TS-19.avi"),
        ("dir+file dotted", "The.Walking.Dead.bdrip_[teko].Season_01.s01e01.Days.Gone.Bye.avi"),
    ];
    for (label, p) in paths { dump(label, p); }
}

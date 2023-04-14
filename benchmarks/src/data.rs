use super::*;

lazy_static::lazy_static!{
    pub static ref DELTA_DISTR: Vec<f64> = {
        let mut delta_distr = vec![0.];
        let mut s = 0.;
        for n in 1..DELTA_DISTR_SIZE {
            let x = n as f64;
            s += 1. / (2. * (x + 3.) * (x.log2() + 2.)*(x.log2() + 2.));
            delta_distr.push(s)
        }
        delta_distr
    };
}

macro_rules! compute_ratio {
    ($data:expr, $table:ident, $len_func:ident) => {{
        let mut total = 0.0;
        for value in &$data {
            if $len_func::<false>(*value) <= $table::READ_BITS as usize {
                total += 1.0;
            }
        }
        total / $data.len() as f64
    }};
}

/// Generate the data needed to benchmark the unary code and return them and the
/// ratio of values that will hit the tables
pub fn gen_unary_data() -> (f64, Vec<u64>) {
    let mut rng = rand::thread_rng();
    
    let unary_data = (0..VALUES)
        .map(|_| {
            let v: u64 = rng.gen();
            v.trailing_zeros() as u64
        })
        .collect::<Vec<_>>();

    let ratio = compute_ratio!(unary_data, unary_tables, len_unary);

    (ratio, unary_data)
}

/// Generate the data needed to benchmark the gamma code and return them and the
/// ratio of values that will hit the tables
pub fn gen_gamma_data() -> (f64, Vec<u64>) {
    let mut rng = rand::thread_rng();
    
    let distr = rand_distr::Zeta::new(2.0).unwrap();
    let gamma_data = (0..VALUES)
        .map(|_| {
            rng.sample(distr) as u64 - 1
        })
        .collect::<Vec<_>>();

    let ratio = compute_ratio!(gamma_data, gamma_tables, len_gamma);

    (ratio, gamma_data)
}

/// Generate the data needed to benchmark the delta code and return them and the
/// ratio of values that will hit the tables
pub fn gen_delta_data() -> (f64, Vec<u64>) {
    let mut rng = rand::thread_rng();

    let distr = rand_distr::Uniform::new(0., DELTA_DISTR[DELTA_DISTR.len() - 1]);
    let delta_data = (0..VALUES)
        .map(|_| {
            let  p = rng.sample(distr);
            let s = DELTA_DISTR.binary_search_by(|v| {
                v.partial_cmp(&p).unwrap()
            });
            match s { Ok(x) => x as u64, Err(x) => x as u64 - 1} 
        })
        .collect::<Vec<_>>();

    let ratio = compute_ratio!(delta_data, delta_tables, len_delta);

    (ratio, delta_data)
}

/// Generate the data needed to benchmark the zeta3 code and return them and the
/// ratio of values that will hit the tables
pub fn gen_zeta3_data() -> (f64, Vec<u64>) {
    let mut rng = rand::thread_rng();

    let distr = rand_distr::Zeta::new(1.2).unwrap();
    let zeta3_data = (0..VALUES)
        .map(|_| {
            rng.sample(distr) as u64 - 1
        })
        .collect::<Vec<_>>();

    let ratio = zeta3_data.iter().map(|value| {
        if len_zeta::<false>(*value, 3) <= zeta_tables::READ_BITS as usize {
            1
        } else {
            0
        }
    }).sum::<usize>() as f64 / VALUES as f64;

    (ratio, zeta3_data)
}

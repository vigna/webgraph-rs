include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
use anyhow::Result;
use std::ffi::{CStr, CString};

impl mph {
    pub fn load(path: &str) -> Result<Self> {
        let c_string: CString = CString::new(path).unwrap();
        let c_str: &CStr = c_string.as_c_str();
        unsafe {
            load_mph(c_str.as_ptr() as *const u8)
                .as_ref()
                .map(|m| *m)
                .ok_or(anyhow::anyhow!("Cannot load MPH"))
        }
    }

    pub fn get(&self, key: &str) -> usize {
        unsafe { mph_get_byte_array(self, key.as_ptr(), key.len() as u64) as usize }
    }
}

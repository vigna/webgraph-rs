/// The logic of the read tables lookup are always the same so this is just a 
/// way to centralize the code. (L2M implementation)
macro_rules! impl_table_call {
    ($self:expr, $USE_TABLE:expr, $tabs:ident, M2L) => {
        if $USE_TABLE {
            if let Ok(idx) = $self.peek_bits($tabs::READ_BITS) {
                let (value, len) = $tabs::READ_M2L[idx as usize];
                if len != $tabs::MISSING_VALUE_LEN {
                    $self.skip_bits(len as u8);
                    return Ok(value as u64);
                }
            }
        }
    };
    ($self:expr, $USE_TABLE:ident, $tabs:ident, L2M) => {
        if $USE_TABLE {
            if let Ok(idx) = $self.peek_bits($tabs::READ_BITS) {
                let (value, len) = $tabs::READ_L2M[idx as usize];
                if len != $tabs::MISSING_VALUE_LEN {
                    $self.skip_bits(len as u8);
                    return Ok(value as u64);
                }
            }
        }
    };
}

pub(crate) use impl_table_call;    
pub struct Limits {
    /// The maximum bits/sec for each connection
    pub max_throughput: u64,

    /// The expected RTT in milliseconds
    pub expected_rtt: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_throughput: 150,
            expected_rtt: 150,
        }
    }
}

impl Limits {
    pub fn limits(&self) -> s2n_quic::provider::limits::Limits {
        let data_window = self.data_window();

        s2n_quic::provider::limits::Limits::default()
            .with_data_window(data_window)
            .unwrap()
            .with_max_send_buffer_size(data_window.min(u32::MAX as _) as _)
            .unwrap()
            .with_bidirectional_local_data_window(data_window)
            .unwrap()
            .with_bidirectional_remote_data_window(data_window)
            .unwrap()
            .with_unidirectional_data_window(data_window)
            .unwrap()
    }

    fn data_window(&self) -> u64 {
        s2n_quic_core::transport::parameters::compute_data_window(
            self.max_throughput,
            core::time::Duration::from_millis(self.expected_rtt),
            2,
        )
        .as_u64()
    }
}

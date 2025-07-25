#include <hls_stream.h>

#include "data/annotated_mux_config.h"
#include "annotated_mux.hpp"

/**
 * Test only the multiplexer
 */
void Testbench_annotated_mux_rr(hls::stream<T0> &in0, hls::stream<T1> &in1, hls::stream<T2> &in2, hls::stream<TO> &out) {
#pragma HLS INTERFACE axis port=in0
#pragma HLS INTERFACE axis port=in1
#pragma HLS INTERFACE axis port=in2
#pragma HLS INTERFACE axis port=out
#pragma HLS INTERFACE ap_ctrl_none port=return
    AnnotatedMultiplex::StreamingNetworkMultiplex<MultiplexStrategy::ROUND_ROBIN, TO_WIDTH, T0, T1, T2>(out, in0, in1, in2);
}

/**
 * Test the entire pipeline
 */
void Testbench_annotated_mux_rr_complete(hls::stream<T0> &in0, hls::stream<T1> &in1, hls::stream<T2> &in2, hls::stream<T0> &out0, hls::stream<T1> &out1, hls::stream<T2> &out2) {
#pragma HLS INTERFACE axis port=in0
#pragma HLS INTERFACE axis port=in1
#pragma HLS INTERFACE axis port=in2
#pragma HLS INTERFACE axis port=out0
#pragma HLS INTERFACE axis port=out1
#pragma HLS INTERFACE axis port=out2
#pragma HLS INTERFACE ap_ctrl_none port=return
    hls::stream<TO> network("tb_network");
    AnnotatedMultiplex::StreamingNetworkMultiplex<MultiplexStrategy::ROUND_ROBIN, TO_WIDTH, T0, T1, T2>(network, in0, in1, in2);
    AnnotatedDemultiplex::StreamingNetworkDeMultiplex<TO_WIDTH, T0, T1, T2>(network, out0, out1, out2); 
}
#include <ap_int.h>
#include <hls_stream.h>

enum class MultiplexStrategy {
    ROUND_ROBIN,
    ROUND_ROBIN_FLEXIBLE,
    LOAD_BALANCE,
    PRIORITY_LIST
};

/**
 * Provide kernels to multiplex data on some strategy with additional information provided.
 * Similar functionality was implemented in concat.hpp, but with slightly different constraints.
 */
class AnnotatedMultiplex {
    public:
        /**
         * Using the given strategy, select one incoming data stream and put it to the destination stream. Before sending the data, send a single
         * frame containing the ID/index of the selected stream.  
         * 
         * \tparam S The strategy to use when multiplexing
         * \tparam W The bitwidth of the output datatype (must be larger than log2(sizeof...(src)))
         * \tparam ...TI Input data types
         * 
         * \param dst The destination hls::stream
         * \param ...src The source stream parameter pack
         */
        template <MultiplexStrategy S = MultiplexStrategy::ROUND_ROBIN, size_t W, typename... TI >
        static void StreamingNetworkMultiplex(hls::stream<ap_int<W>> &dst, hls::stream<TI> &...src) {
            // ap_int public method
            AnnotatedMultiplex::StreamingAnnotatedMultiplex_impl<ap_int<W>, S, W, TI...>(dst, src...);
        }

        /**
         * Using the given strategy, select one incoming data stream and put it to the destination stream. Before sending the data, send a single
         * frame containing the ID/index of the selected stream.  
         * 
         * \tparam S The strategy to use when multiplexing
         * \tparam W The bitwidth of the output datatype (must be larger than log2(sizeof...(src)))
         * \tparam ...TI Input data types
         * 
         * \param dst The destination hls::stream
         * \param ...src The source stream parameter pack
         */
        template <MultiplexStrategy S = MultiplexStrategy::ROUND_ROBIN, size_t W, typename... TI >
        static void StreamingNetworkMultiplex(hls::stream<ap_uint<W>> &dst, hls::stream<TI> &...src) {
            // ap_uint public method
            AnnotatedMultiplex::StreamingAnnotatedMultiplex_impl<ap_uint<W>, S, W, TI...>(dst, src...);
        }

    private:
        /** Actual implementation of the streamed annotated multiplex */
        template <typename TO, MultiplexStrategy S, size_t OUT_WIDTH, typename... TI>
        static void StreamingAnnotatedMultiplex_impl(hls::stream<TO> &dst, hls::stream<TI> &...src) {
            constexpr unsigned int header_width = clog2(sizeof...(src));
            constexpr unsigned int N = sizeof...(TI);
            static_assert(OUT_WIDTH >= header_width, "The output datawidth must be wide enough to represent the ID of every channel!");

            // TODO: Remove
            static_assert(S == MultiplexStrategy::ROUND_ROBIN, "Other multiplex strategies than RR not supported yet!");

            // Gets added to every transmission to identify the original sender
            static ap_uint<OUT_WIDTH> sel = 0;

            // TODO: Implement arbiting logic
            // Use or subclass from finn-hlslib/concat.hpp/PackReader, since we need very similar functionality
        }
};
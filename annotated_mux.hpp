#include <ap_int.h>
#include <hls_stream.h>

// Use existing functionality from PackReader
#include "concat.hpp"

enum class MultiplexStrategy {
    ROUND_ROBIN,
    ROUND_ROBIN_BLOCKING,
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
            static_assert(S == MultiplexStrategy::ROUND_ROBIN || S == MultiplexStrategy::ROUND_ROBIN_BLOCKING, "Other multiplex strategies than RR not supported yet!");

            // Gets added to every transmission to identify the original sender
            static ap_uint<OUT_WIDTH> sel = 0;
            static PackReader<0, TI...> reader;

            // Select next candidate based on the strategy chosen
            if (S == MultiplexStrategy::ROUND_ROBIN || S == MultiplexStrategy::ROUND_ROBIN_BLOCKING) {
                bool can_read = false;
                unsigned int count = 0;
                while(!can_read) {
                    can_read = reader.read_nb(sel, content, src...);

                    // If we arent blocking, move on regardless of whether the read was successful
                    if (S == MultiplexStrategy:ROUND_ROBIN) {
                        sel = (sel + 1) % N;
                        count++;
                    }

                    // Have searched the entire array of input streams and didnt find anything
                    if (count == N-1) {
                        return;
                    }

                    // Write annotated data to dst 
                    if (can_read) {
                        dst.write(sel);
                        dst.write(content);
                        return;
                    }
                    if (S == MultiplexStrategy::ROUND_ROBIN_BLOCKING) {
                        return;
                    }
                }
            }
        }
};
#include <ap_int.h>
#include <hls_stream.h>

// Use existing functionality from PackReader
#include "concat.hpp"

// Use existing functionality from PackWriter
#include "split.hpp"

enum class MultiplexStrategy {
    ROUND_ROBIN,
    ROUND_ROBIN_BLOCKING,
    LOAD_BALANCE,
    PRIORITY_LIST
};


// TODO: Currently inefficiently implemented, since every frame needs twice the frames now. Can be
// improved via bit exact packing

// TODO: Test with signed ints as well

// TODO: Test edgecases

/**
 * Provide kernels to multiplex data on some strategy with additional information provided.
 * Similar functionality was implemented in concat.hpp, but with slightly different constraints.
 */
class AnnotatedMultiplex {
    public:
        /**
         * Check if the output stream can hold both the header and the stream size itself
         */
        static constexpr bool enough_space(const size_t OUT_WIDTH, const size_t HEADER_WIDTH, size_t current_size) {
            return (OUT_WIDTH >= (current_size + HEADER_WIDTH));
        }

        /**
        * Given a total output width of a multiplexer and the widths of all input streams, calculate whether the
        * output can, for any stream, keep both the header as well as the data itself in one frame
        */
        template<typename ...S>
        static constexpr bool enough_space(const size_t OUT_WIDTH, const size_t HEADER_WIDTH, size_t current_size, S ...others) {
            return (OUT_WIDTH >= (current_size + HEADER_WIDTH)) && enough_space(OUT_WIDTH, HEADER_WIDTH, others...);
        }
        /**
         * Using the given strategy, select one incoming data stream and put it to the destination stream. Before sending the data, send a single
         * frame containing the ID/index of the selected stream.  
         * 
         * \warning We cannot check that the output datawidth is large enough to support both the header and the data itself. This needs to be checked in FINN or with AnnotatedMultiplex::enough_space().
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
         * \warning We cannot check that the output datawidth is large enough to support both the header and the data itself. This needs to be checked in FINN or with AnnotatedMultiplex::enough_space().
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
        /**
         * Insert the header at the most significant position in the output data frame.
         */
        template<typename T, size_t TW, typename H, size_t HW>
        static TO with_header(TO content, H) {
            return content | (static_cast<TO>(header) << (TW-HW));
        }

        /** Actual implementation of the streamed annotated multiplex */
        template <typename TO, MultiplexStrategy S, size_t OUT_WIDTH, typename... TI>
        static void StreamingAnnotatedMultiplex_impl(hls::stream<TO> &dst, hls::stream<TI> &...src) {
            constexpr unsigned int N = sizeof...(TI);
            constexpr unsigned int header_width = clog2(N);
            static_assert(OUT_WIDTH >= header_width, "The output datawidth must be wide enough to represent the ID of every channel!");

            // TODO: Remove
            static_assert(S == MultiplexStrategy::ROUND_ROBIN, "Other multiplex strategies than RR not tested yet!");

            // Gets added to every transmission to identify the original sender
            static ap_uint<header_width> sel = 0;
            static PackReader<0, TI...> reader;
            unsigned int count = 0;

            #pragma HLS reset variable=sel
            #pragma HLS reset variable=reader

            // Select next candidate based on the strategy chosen
            if (S == MultiplexStrategy::ROUND_ROBIN || S == MultiplexStrategy::ROUND_ROBIN_BLOCKING) {
                bool can_read = false;
                TO content;
                while(!can_read) {
                    can_read = reader.read_nb(sel, content, src...);

                    // Have searched the entire array of input streams and didnt find anything
                    if (count == N) {
                        return;
                    }

                    // Write annotated data to dst 
                    if (can_read) {
                        content = AnnotatedMultiplex::with_header<TO, OUT_WIDTH, decltype(sel), header_width>(content, sel);
                        dst.write(content);
                        sel = (sel + 1) % N;
                        return;
                    }
                    if (S == MultiplexStrategy::ROUND_ROBIN_BLOCKING) {
                        return;
                    } else {
                        // Case: Didnt find data, but are not blocking, so check next stream
                        sel = (sel + 1) % N;
                        count++;
                    }
                }
            }
        }
};

/**
 * Demux data coming from a stream that is written to by a AnnotatedMultiplex kernel
 */
class AnnotatedDemultiplex {
    public:
        /**
         * \brief Demultiplex based on the source stream ID of the src stream.
         * 
         * \tparam W The width of the incoming datatype
         * \tparam ...TO The output datatypes
         * \param src The incoming hls stream that is supplied by an AnnotatedMultiplex kernel
         * \param ...dst The destination streams to demultiplex to. 
         */
        template <size_t W, typename ...TO >
        static void StreamingNetworkDeMultiplex(hls::stream<ap_uint<W>> &src, hls::stream<TO> &...dst) {
            // ap_uint public method
            AnnotatedDemultiplex::StreamingAnnotatedDeMultiplex_impl<ap_uint<W>, W, TO...>(src, dst...);
        }

        /**
         * \brief Demultiplex based on the source stream ID of the src stream.
         * 
         * \tparam W The width of the incoming datatype
         * \tparam ...TO The output datatypes
         * \param src The incoming hls stream that is supplied by an AnnotatedMultiplex kernel
         * \param ...dst The destination streams to demultiplex to. 
         */
        template <size_t W, typename ...TO >
        static void StreamingNetworkDeMultiplex(hls::stream<ap_int<W>> &src, hls::stream<TO> &...dst) {
            // ap_int public method
            AnnotatedDemultiplex::StreamingAnnotatedDeMultiplex_impl<ap_int<W>, W, TO...>(src, dst...);
        }

    private:
        /** Actual implementation of the streamed annotated demultiplex */
        template<typename TI, size_t IN_WIDTH, typename ...TO>
        static void StreamingAnnotatedDeMultiplex_impl(hls::stream<TI> &src, hls::stream<TO> &...dst) {
            constexpr unsigned int N = sizeof...(dst);
            constexpr unsigned int header_width = clog2(N);
            static_assert(header_width <= IN_WIDTH, "Cannot demultiplex. Too many streams to identify with the given incoming bitwidth!");
            static PackWriter<0, TO...> writer;
            TI header;
            TI content;
            if (src.read_nb(header)) {
                content = src.read();
                writer.write((unsigned int) header, content, dst...);
            } 
        }
};
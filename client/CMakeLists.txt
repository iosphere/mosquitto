include_directories(${mosquitto_SOURCE_DIR}/lib
			${STDBOOL_H_PATH} ${STDINT_H_PATH})
link_directories(${mosquitto_BINARY_DIR}/lib)

set(shared_src client_shared.c client_shared.h)

add_executable(mosquitto_pub pub_client.c ${shared_src})
add_executable(mosquitto_sub sub_client.c ${shared_src})

target_link_libraries(mosquitto_pub mosquitto)
target_link_libraries(mosquitto_sub mosquitto)

install(TARGETS mosquitto_pub RUNTIME DESTINATION ${BINDIR} LIBRARY DESTINATION ${LIBDIR})
install(TARGETS mosquitto_sub RUNTIME DESTINATION ${BINDIR} LIBRARY DESTINATION ${LIBDIR})

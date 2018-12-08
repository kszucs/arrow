cmake_minimum_required(VERSION 2.7)
project(pyarrow_c_example)

add_executable(example example.c)

# Running from a Python sdist tarball
set(LOCAL_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/../../cmake_modules")
if (EXISTS "${LOCAL_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${LOCAL_CMAKE_MODULES})
endif()

find_package(Arrow REQUIRED)
find_package(PythonLibsNew REQUIRED)
execute_process(COMMAND python -c "import pyarrow as pa; print(' '.join(pa.get_library_dirs()))"
                OUTPUT_VARIABLE PYARROW_INCLUDE)

message(STATUS ${ARROW_INCLUDE_DIRS})
message(STATUS ${PYTHON_INCLUDE_DIRS})
message(STATUS ${PYARROW_INCLUDE})

include_directories(SYSTEM ${PYTHON_INCLUDE_DIRS} ${ARROW_INCLUDE_DIR}, ${PYARROW_INCLUDE})

install(TARGETS example DESTINATION bin)

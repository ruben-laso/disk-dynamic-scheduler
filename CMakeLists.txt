cmake_minimum_required(VERSION 3.14) # Minimum for FetchContent
project(fonda_sched CXX)
enable_language(C)

# --- 1. Options & Flags ---
option(PEDANTIC_FLAGS "Enable pedantic compiler flags" OFF)
option(WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)
option(FONDA_BUILD_MEMDAG "Build external tool dagp" ON)
option(FONDA_UNIT_TESTS "Build unit tests" ON)

set(FONDA_CXX_STANDARD "17" CACHE STRING "CXX Version")

if (PEDANTIC_FLAGS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic -Wall -Wextra")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pedantic -Wall -Wextra")
endif ()

if (WARNINGS_AS_ERRORS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

# --- 2. External Dependencies (Memdag) ---
if (FONDA_BUILD_MEMDAG)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GraphViz REQUIRED IMPORTED_TARGET libgvc)

    set(MEMDAG_SOURCE_DIR ${PROJECT_SOURCE_DIR}/extlibs/memdag/src/)
    add_library(memdag STATIC
            ${MEMDAG_SOURCE_DIR}/memdag.cpp
            ${MEMDAG_SOURCE_DIR}/sp-ization.cpp
            ${MEMDAG_SOURCE_DIR}/graph-algorithms.cpp
            ${MEMDAG_SOURCE_DIR}/tree.cpp
            ${MEMDAG_SOURCE_DIR}/fifo.cpp
            ${MEMDAG_SOURCE_DIR}/heap.cpp
            ${MEMDAG_SOURCE_DIR}/sp-recognition.cpp
            ${MEMDAG_SOURCE_DIR}/graph.cpp
            ${MEMDAG_SOURCE_DIR}/sp-traversals.cpp
            ${MEMDAG_SOURCE_DIR}/tree-opt-sched.cpp
            ${MEMDAG_SOURCE_DIR}/tree-postorder-sched.cpp
            ${MEMDAG_SOURCE_DIR}/cluster.cpp
    )

    target_include_directories(memdag PUBLIC
            ${MEMDAG_SOURCE_DIR}
            extlibs/csv/single_include
    )
    target_link_libraries(memdag PRIVATE PkgConfig::GraphViz)
    set_target_properties(memdag PROPERTIES CXX_STANDARD ${FONDA_CXX_STANDARD})
endif ()

# --- 3. The Core Logic Library (The "my_lib") ---
# We group all your scheduler files here so they can be shared
# between the main app and the tests.
add_library(fonda_logic STATIC
        "${PROJECT_SOURCE_DIR}/fonda_scheduler/io/graphWeightsBuilder.cpp"
        "${PROJECT_SOURCE_DIR}/fonda_scheduler/common.cpp"
        "${PROJECT_SOURCE_DIR}/fonda_scheduler/OfflineSchedulerAndBaseline.cpp"
        "${PROJECT_SOURCE_DIR}/fonda_scheduler/OfflineSchedulerHelpers.cpp"
        "${PROJECT_SOURCE_DIR}/fonda_scheduler/RuntimeSystem.cpp"
        "${PROJECT_SOURCE_DIR}/fonda_scheduler/OnlineScheduler.cpp"
)

target_include_directories(fonda_logic PUBLIC
        "${PROJECT_SOURCE_DIR}/include"
        "${PROJECT_SOURCE_DIR}/extlibs"
)

target_link_libraries(fonda_logic PUBLIC memdag)
set_target_properties(fonda_logic PROPERTIES CXX_STANDARD ${FONDA_CXX_STANDARD})

# --- 4. Main Executable ---
add_executable(fonda_scheduler "${PROJECT_SOURCE_DIR}/fonda_scheduler/main.cpp")
target_link_libraries(fonda_scheduler PRIVATE fonda_logic)

# --- 5. Testing Section ---
if (FONDA_UNIT_TESTS)
    enable_testing()

    # Automatically download GTest/GMock
    include(FetchContent)
    FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/refs/heads/main.zip
    )
    # Prevents issues on Windows with compiler settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    # Define the test runner
    add_executable(dagp-tests
            "${PROJECT_SOURCE_DIR}/test/test-singular-functions.cpp"
            "${PROJECT_SOURCE_DIR}/test/test-general.cpp"
            "${PROJECT_SOURCE_DIR}/test/test-moving-events.cpp"
            "${PROJECT_SOURCE_DIR}/test/WorkflowFactory.cpp"
            "${PROJECT_SOURCE_DIR}/test/ClusterFactory.cpp"
            "${PROJECT_SOURCE_DIR}/test/EventFactory.cpp"
            # Add your other test .cpp files here
    )

    # Link to our core logic + GTest + GMock
    target_link_libraries(dagp-tests
            PRIVATE
            fonda_logic
            GTest::gtest_main
            GTest::gmock
    )

    # Register the tests with CTest
    include(GoogleTest)
    gtest_discover_tests(dagp-tests)

   # if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
   #     target_compile_options(fonda_logic PRIVATE --coverage -fno-inline -fno-inline-small-functions -fno-default-inline)
   #     target_link_options(dagp-tests PRIVATE --coverage)
   # endif()
endif ()
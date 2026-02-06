include(FetchContent)

FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/heads/main.zip
)

# For Windows: prevents gtest from overriding your compiler settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

# Create the test executable
add_executable(unit_tests test_main.cpp)

# Link against gtest, gmock, and your project library
target_link_libraries(unit_tests
        GTest::gtest_main
        GTest::gmock
        my_lib
)

# This allows you to run 'ctest' from the command line
include(GoogleTest)
gtest_discover_tests(unit_tests)
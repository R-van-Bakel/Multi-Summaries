#include <exception>
#include <string>

#include "../include/my_exception.hpp"

MyException::MyException(const std::string &err) : message(err) {}

const char *MyException::what() const noexcept
{
    return message.c_str();
}

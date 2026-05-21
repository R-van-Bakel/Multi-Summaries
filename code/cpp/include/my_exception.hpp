#pragma once
#include <exception>
#include <string>

class MyException : public std::exception
{
private:
    std::string message;

public:
    explicit MyException(const std::string &err);

    const char *what() const noexcept override;
};
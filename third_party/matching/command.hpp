#pragma once

#include "matching/types.hpp"

#include <charconv>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace matching {

enum class Action { Submit, Cancel, Amend, Book, Orders, Check, Help, Quit };

struct Command {
    Action action{Action::Submit};
    NewOrder order;

    template <typename EngineType>
    Outcome apply(EngineType& engine) const {
        switch (action) {
        case Action::Submit: return engine.submit(order);
        case Action::Cancel: return engine.cancel(order.id);
        case Action::Amend: return engine.amend(order.id, order.price, order.quantity);
        default: throw std::logic_error("not a book mutation");
        }
    }
};

template <typename Number>
Number parse_number(std::string_view text) {
    Number number{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), number);
    if (error != std::errc{} || end != text.data() + text.size())
        throw std::invalid_argument("invalid integer: " + std::string(text));
    return number;
}

inline Side parse_side(std::string_view text) {
    if (text == "BUY") return Side::Buy;
    if (text == "SELL") return Side::Sell;
    throw std::invalid_argument("side must be BUY or SELL");
}

// Parsing and console I/O are deliberately outside the matching core.
inline std::optional<Command> parse_command(std::string line) {
    const auto comment = line.find('#');
    if (comment != std::string::npos) line.resize(comment);
    std::istringstream stream(line);
    std::vector<std::string> tokens;
    for (std::string token; stream >> token;) tokens.push_back(std::move(token));
    if (tokens.empty()) return std::nullopt;

    Command command;
    if (tokens[0] == "LIMIT" && tokens.size() == 5) {
        command.order = {parse_number<OrderId>(tokens[1]), parse_side(tokens[2]),
            OrderType::Limit, parse_number<Price>(tokens[3]), parse_number<Quantity>(tokens[4])};
    } else if (tokens[0] == "MARKET" && tokens.size() == 4) {
        command.order = {parse_number<OrderId>(tokens[1]), parse_side(tokens[2]),
            OrderType::Market, 0, parse_number<Quantity>(tokens[3])};
    } else if (tokens[0] == "CANCEL" && tokens.size() == 2) {
        command.action = Action::Cancel;
        command.order.id = parse_number<OrderId>(tokens[1]);
    } else if (tokens[0] == "AMEND" && tokens.size() == 4) {
        command.action = Action::Amend;
        command.order.id = parse_number<OrderId>(tokens[1]);
        command.order.price = parse_number<Price>(tokens[2]);
        command.order.quantity = parse_number<Quantity>(tokens[3]);
    } else if (tokens.size() == 1) {
        if (tokens[0] == "BOOK") command.action = Action::Book;
        else if (tokens[0] == "ORDERS") command.action = Action::Orders;
        else if (tokens[0] == "CHECK") command.action = Action::Check;
        else if (tokens[0] == "HELP") command.action = Action::Help;
        else if (tokens[0] == "QUIT") command.action = Action::Quit;
        else throw std::invalid_argument("unknown command; use HELP");
    } else {
        throw std::invalid_argument("wrong command or argument count; use HELP");
    }
    return command;
}

} // namespace matching

#pragma once
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <functional>
#include <vector>
#include <string_view>
#include <iostream> // DEBUG.
// -------------------------------------------------------------------------------------
struct NonCopyable {
   NonCopyable() = default;
   NonCopyable(const NonCopyable &) = delete;
   NonCopyable &operator=(const NonCopyable &) = delete;
   NonCopyable(NonCopyable &&) = default;
   NonCopyable &operator=(NonCopyable &&) = default;
};
// -------------------------------------------------------------------------------------
// Remove from vector where the condition holds (erase-remove-idiom)
// Usage: Utility::EraseRemove<int>(vec, [](auto &iter) { return iter == 5; });
template <class T>
static void EraseRemove(std::vector<T> &vec, std::function<bool(const T &)> cond)
{
   // Note: dont use std::remove_if, it does not guarantee the order in which elements are inspected.
   auto last = vec.end();
   auto write_iter = std::find_if(vec.begin(), last, cond);
   if (write_iter != last) {
      for (auto read_iter = write_iter; ++read_iter != last;) {
         if (!cond(*read_iter)) {
            *write_iter++ = std::move(*read_iter);
         }
      }
   }

   vec.erase(write_iter, vec.end());
}
// -------------------------------------------------------------------------------------
inline std::vector<std::string_view> SplitPattern(std::string_view pattern) {
   std::vector<std::string_view> ret;
   std::size_t start = 0;
   // std::cerr << "pattern=" << pattern << std::endl;
   while (start <= pattern.size()) {
      auto end = pattern.find('%', start);
      // std::cerr << "start=" << start << " end=" << end << std::endl;
      if (end == std::string_view::npos) end = pattern.size();
      auto local_pattern = pattern.substr(start, end - start);
      if (!local_pattern.empty())
         ret.emplace_back(local_pattern);
      start = end + 1;
   }
   return ret;
}
// -------------------------------------------------------------------------------------
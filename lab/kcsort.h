/*************************************************************************************************
 * Sort utilities.
 *************************************************************************************************/


#ifndef _KCSORT_H                        // duplication check
#define _KCSORT_H

#include <utility>
#include <algorithm>
#include <functional>


/**
 * Sort an array by insertion sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE, class LESS>
inline void insertsort(TYPE* ary, size_t nmemb, LESS less) {
  for (size_t i = 1; i < nmemb; i++) {
    size_t j = i;
    while (j >= 1 && less(ary[j], ary[j-1])) {
      std::swap(ary[j], ary[j-1]);
      j--;
    }
  }
}


/**
 * Sort an array by insertion sort.
 * @param TYPE the type of each element of the array.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE>
inline void insertsort(TYPE* ary, size_t nmemb) {
  insertsort(ary, nmemb, std::less<TYPE>());
}


/**
 * Sort an array by shell sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE, class LESS>
inline void shellsort(TYPE* ary, size_t nmemb, LESS less) {
  size_t gap = 1;
  size_t max = nmemb / 9;
  while (gap < max) {
    gap = gap * 3 + 1;
  }
  while (gap > 0) {
    for (size_t i = gap; i < nmemb; i++) {
      size_t j = i;
      while (j >= gap && less(ary[j], ary[j-gap])) {
        std::swap(ary[j], ary[j-gap]);
        j -= gap;
      }
    }
    gap /= 3;
  }
}


/**
 * Sort an array by shell sort.
 * @param TYPE the type of each element of the array.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE>
inline void shellsort(TYPE* ary, size_t nmemb) {
  shellsort(ary, nmemb, std::less<TYPE>());
}


/**
 * Sort an array by comb sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE, class LESS>
inline void combsort(TYPE* ary, size_t nmemb, LESS less) {
  size_t gap = nmemb;
  bool swapped = true;
  while ((gap > 1) || swapped) {
    if (gap > 1) gap = gap / 1.3;
    if (gap == 9 || gap == 10) gap = 11;
    swapped = false;
    size_t i = 0;
    while (i + gap < nmemb) {
      if (less(ary[i+gap], ary[i])) {
        std::swap(ary[i], ary[i+gap]);
        swapped = true;
      }
      i++;
    }
  }
}


/**
 * Sort an array by comb sort.
 * @param TYPE the type of each element of the array.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE>
inline void combsort(TYPE* ary, size_t nmemb) {
  combsort(ary, nmemb, std::less<TYPE>());
}


/**
 * Sort an array by heap sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE, class LESS>
inline void heapsort(TYPE* ary, size_t nmemb, LESS less) {
  if (nmemb < 2) return;
  nmemb--;
  size_t bot = nmemb;
  while (true) {
    size_t pidx = bot;
    TYPE pv = ary[bot];
    while (true) {
      size_t cidx = pidx * 2 + 1;
      if (cidx > nmemb) break;
      if (cidx != nmemb && less(ary[cidx], ary[cidx+1])) cidx++;
      if (!less(pv, ary[cidx])) break;
      ary[pidx] = ary[cidx];
      pidx = cidx;
    }
    ary[pidx] = pv;
    if (bot < 1) break;
    bot--;
  }
  while (nmemb > 0) {
    std::swap(ary[0], ary[nmemb]);
    size_t pidx = 0;
    TYPE pv = ary[0];
    while (true) {
      size_t cidx = pidx * 2 + 1;
      if (cidx > nmemb - 1) break;
      if (cidx != nmemb - 1 && less(ary[cidx], ary[cidx+1])) cidx++;
      if (!less(pv, ary[cidx])) break;
      ary[pidx] = ary[cidx];
      pidx = cidx;
    }
    ary[pidx] = pv;
    nmemb--;
  }
}


/**
 * Sort an array by heap sort.
 * @param TYPE the type of each element of the array.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE>
inline void heapsort(TYPE* ary, size_t nmemb) {
  heapsort(ary, nmemb, std::less<TYPE>());
}


/**
 * Sort an array by quick sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE, class LESS>
inline void quicksort(TYPE* ary, size_t nmemb, LESS less) {
  if (nmemb < 2) return;
  struct { size_t low; size_t high; } stack[48];
  stack[0].low = 0;
  stack[0].high = nmemb - 1;
  size_t sidx = 1;
  while (sidx > 0) {
    sidx--;
    size_t low = stack[sidx].low;
    size_t high = stack[sidx].high;
    if (low + 10 > high) {
      while (++low <= high) {
        size_t lidx = low;
        while (lidx >= 1 && less(ary[lidx], ary[lidx-1])) {
          std::swap(ary[lidx], ary[lidx-1]);
          lidx--;
        }
      }
    } else {
      size_t lidx = low;
      size_t hidx = high;
      size_t midx = (high + low) / 2;
      if (less(ary[midx], ary[hidx])) {
        if (less(ary[hidx], ary[lidx])) {
          midx = hidx;
        } else  if (less(ary[midx], ary[lidx])) {
          midx = lidx;
        }
      } else {
        if (less(ary[lidx], ary[hidx])) {
          midx = hidx;
        } else if (less(ary[lidx], ary[midx])) {
          midx = lidx;
        }
      }
      if (midx != hidx) std::swap(ary[midx], ary[hidx]);
      TYPE pv = ary[hidx];
      while (true) {
        while (less(ary[lidx], pv)) {
          lidx++;
        }
        hidx--;
        while (lidx < hidx && less(pv, ary[hidx])) {
          hidx--;
        }
        if (lidx >= hidx) break;
        std::swap(ary[lidx], ary[hidx]);
      }
      std::swap(ary[lidx], ary[high]);
      if (lidx - low < high - lidx) {
        stack[sidx].low = lidx + 1;
        stack[sidx++].high = high;
        if (low < lidx) {
          stack[sidx].low = low;
          stack[sidx++].high = lidx - 1;
        }
      } else {
        stack[sidx].low = low;
        stack[sidx++].high = lidx - 1;
        if (lidx + 1 < high) {
          stack[sidx].low = lidx + 1;
          stack[sidx++].high = high;
        }
      }
    }
  }
}


/**
 * Sort an array by quick sort.
 * @param TYPE the type of each element of the array.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE>
inline void quicksort(TYPE* ary, size_t nmemb) {
  quicksort(ary, nmemb, std::less<TYPE>());
}


/**
 * Sort an array by merge sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE, class LESS>
inline void mergesort(TYPE* ary, size_t nmemb, LESS less) {
  if (nmemb < 2) return;
  TYPE* tmp = new TYPE[nmemb];
  struct {
    LESS less;
    void sort(TYPE* ary, TYPE* tmp, size_t low, size_t high) {
      if (low >= high) return;
      size_t mid = (low + high) / 2;
      sort(ary, tmp, low, mid);
      sort(ary, tmp, mid + 1, high);
      for (size_t i = low; i <= mid; i++) {
        tmp[i] = ary[i];
      }
      size_t bot = high;
      for (size_t i = mid + 1; i <= high; i++) {
        tmp[i] = ary[bot--];
      }
      size_t top = low;
      bot = high;
      for (size_t k = low; k <= high; k++) {
        if (less(tmp[bot], tmp[top])) {
          ary[k] = tmp[bot--];
        } else {
          ary[k] = tmp[top++];
        }
      }
    }
  } func;
  func.less = less;
  func.sort(ary, tmp, 0, nmemb - 1);
  delete[] tmp;
}


/**
 * Sort an array by merge sort.
 * @param TYPE the type of each element of the array.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE>
inline void mergesort(TYPE* ary, size_t nmemb) {
  mergesort(ary, nmemb, std::less<TYPE>());
}


/**
 * Sort an array by top N heap sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE, class LESS>
inline void topsort_heap(TYPE* ary, size_t nmemb, size_t top, LESS less) {
  if (nmemb < 2 || top < 1) return;
  if (top > nmemb) top = nmemb;
  size_t cur = 1;
  while (cur < top) {
    size_t cidx = cur;
    while (cidx > 0) {
      size_t pidx = (cidx - 1) / 2;
      if (!less(ary[pidx], ary[cidx])) break;
      std::swap(ary[cidx], ary[pidx]);
      cidx = pidx;
    }
    cur++;
  }
  while (cur < nmemb) {
    if (less(ary[cur], *ary)) {
      std::swap(ary[0], ary[cur]);
      size_t pidx = 0;
      size_t bot = top / 2;
      while (pidx < bot) {
        size_t cidx = pidx * 2 + 1;
        if (cidx < top - 1 && less(ary[cidx], ary[cidx+1])) cidx++;
        if (less(ary[cidx], ary[pidx])) break;
        std::swap(ary[pidx], ary[cidx]);
        pidx = cidx;
      }
    }
    cur++;
  }
  cur = top - 1;
  while (cur > 0){
    std::swap(ary[0], ary[cur]);
    size_t pidx = 0;
    size_t bot = cur / 2;
    while (pidx < bot){
      size_t cidx = pidx * 2 + 1;
      if (cidx < cur - 1 && less(ary[cidx], ary[cidx+1])) cidx++;
      if (less(ary[cidx], ary[pidx])) break;
      std::swap(ary[pidx], ary[cidx]);
      pidx = cidx;
    }
    cur--;
  }
}


/**
 * Sort an array by top N heap sort.
 * @param TYPE the type of each element of the array.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE>
inline void topsort_heap(TYPE* ary, size_t nmemb, size_t top) {
  topsort_heap(ary, nmemb, top, std::less<TYPE>());
}


/**
 * Sort an array by top N quick sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE, class LESS>
inline void topsort_quick(TYPE* ary, size_t nmemb, size_t top, LESS less) {
  if (nmemb < 2 || top < 1) return;
  if (top > nmemb) top = nmemb;
  struct { size_t low; size_t high; } stack[48];
  stack[0].low = 0;
  stack[0].high = nmemb - 1;
  size_t sidx = 1;
  while (sidx > 0) {
    sidx--;
    size_t low = stack[sidx].low;
    size_t high = stack[sidx].high;
    if (low > top) continue;
    if (low + 10 > high) {
      while (++low <= high) {
        size_t lidx = low;
        while (lidx >= 1 && less(ary[lidx], ary[lidx-1])) {
          std::swap(ary[lidx], ary[lidx-1]);
          lidx--;
        }
      }
    } else {
      size_t lidx = low;
      size_t hidx = high;
      size_t midx = (high + low) / 2;
      if (less(ary[midx], ary[hidx])) {
        if (less(ary[hidx], ary[lidx])) {
          midx = hidx;
        } else  if (less(ary[midx], ary[lidx])) {
          midx = lidx;
        }
      } else {
        if (less(ary[lidx], ary[hidx])) {
          midx = hidx;
        } else if (less(ary[lidx], ary[midx])) {
          midx = lidx;
        }
      }
      if (midx != hidx) std::swap(ary[midx], ary[hidx]);
      TYPE pv = ary[hidx];
      while (true) {
        while (less(ary[lidx], pv)) {
          lidx++;
        }
        hidx--;
        while (lidx < hidx && less(pv, ary[hidx])) {
          hidx--;
        }
        if (lidx >= hidx) break;
        std::swap(ary[lidx], ary[hidx]);
      }
      std::swap(ary[lidx], ary[high]);
      if (lidx - low < high - lidx) {
        stack[sidx].low = lidx + 1;
        stack[sidx++].high = high;
        if (low < lidx) {
          stack[sidx].low = low;
          stack[sidx++].high = lidx - 1;
        }
      } else {
        stack[sidx].low = low;
        stack[sidx++].high = lidx - 1;
        if (lidx + 1 < high) {
          stack[sidx].low = lidx + 1;
          stack[sidx++].high = high;
        }
      }
    }
  }
}


/**
 * Sort an array by top N quick sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 */
template <class TYPE>
inline void topsort_quick(TYPE* ary, size_t nmemb, size_t top) {
  topsort_quick(ary, nmemb, top, std::less<TYPE>());
}


/**
 * Sort an array by top N sort.
 * @param TYPE the type of each element of the array.
 * @param LESS a functor to compare two elements and return true if the former is less.
 * @param ary the array.
 * @param nmemb the number of elements.
 * @param top the number of top elements, which is to be placed at the head and sorted.
 */
template <class TYPE, class LESS>
inline void topsort(TYPE* ary, size_t nmemb, size_t top, LESS less) {
  if (top < std::pow(nmemb, 0.7)) {
    topsort_heap(ary, nmemb, top, less);
  } else {
    topsort_quick(ary, nmemb, top, less);
  }
}


/**
 * Sort an array by top N sort.
 * @param TYPE the type of each element of the array.
 * @param ary the array.
 * @param nmemb the number of elements.
 * @param top the number of top elements, which is to be placed at the head and sorted.
 */
template <class TYPE>
inline void topsort(TYPE* ary, size_t nmemb, size_t top) {
  topsort(ary, nmemb, top, std::less<TYPE>());
}


#endif                                   // duplication check

// END OF FILE

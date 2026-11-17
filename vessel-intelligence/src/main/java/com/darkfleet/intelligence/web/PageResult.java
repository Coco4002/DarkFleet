package com.darkfleet.intelligence.web;

import java.util.List;

/**
 * One page of a list. {@code hasMore} says whether a next page exists; there is no total count,
 * which would cost a second query on large tables.
 */
public record PageResult<T>(List<T> items, int page, int size, boolean hasMore) {

    public static final int MAX_SIZE = 200;
    public static final int DEFAULT_SIZE = 50;

    /** Checks the requested page; throws 400 when it is out of range. */
    public static void check(int page, int size) {
        if (page < 0) {
            throw ApiException.badRequest("page must be 0 or more");
        }
        if (size < 1 || size > MAX_SIZE) {
            throw ApiException.badRequest("size must be between 1 and " + MAX_SIZE);
        }
    }

    /** SQL suffix fetching one row more than the page, to know whether another page follows. */
    public static String limit(int page, int size) {
        return " LIMIT " + (size + 1) + " OFFSET " + ((long) page * size);
    }

    /** Builds the page from rows fetched with {@link #limit}. */
    public static <T> PageResult<T> of(List<T> rows, int page, int size) {
        boolean more = rows.size() > size;
        return new PageResult<>(more ? rows.subList(0, size) : rows, page, size, more);
    }
}

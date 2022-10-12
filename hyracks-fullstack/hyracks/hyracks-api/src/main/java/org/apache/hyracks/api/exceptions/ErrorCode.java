/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.exceptions;

import org.apache.hyracks.api.util.ErrorMessageUtil;

/**
 * A registry of runtime/compile error codes
 * Error code:
 * 0 --- 9999: runtime errors
 * 10000 ---- 19999: compilation errors
 */
public enum ErrorCode implements IError {
    // Runtime error codes.
    INVALID_OPERATOR_OPERATION(1),
    ERROR_PROCESSING_TUPLE(2),
    FAILURE_ON_NODE(3),
    FILE_WITH_ABSOULTE_PATH_NOT_WITHIN_ANY_IO_DEVICE(4),
    FULLTEXT_PHRASE_FOUND(5),
    JOB_QUEUE_FULL(6),
    INVALID_NETWORK_ADDRESS(7),
    INVALID_INPUT_PARAMETER(8),
    JOB_REQUIREMENTS_EXCEED_CAPACITY(9),
    NO_SUCH_NODE(10),
    CLASS_LOADING_ISSUE(11),
    ILLEGAL_WRITE_AFTER_FLUSH_ATTEMPT(12),
    DUPLICATE_IODEVICE(13),
    NESTED_IODEVICES(14),
    MORE_THAN_ONE_RESULT(15),
    RESULT_FAILURE_EXCEPTION(16),
    RESULT_FAILURE_NO_EXCEPTION(17),
    INCONSISTENT_RESULT_METADATA(18),
    CANNOT_DELETE_FILE(19),
    NOT_A_JOBID(20),
    ERROR_FINDING_DEPLOYED_JOB(21),
    DUPLICATE_DEPLOYED_JOB(22),
    DEPLOYED_JOB_FAILURE(23),
    NO_RESULT_SET(24),
    JOB_CANCELED(25),
    NODE_FAILED(26),
    FILE_IS_NOT_DIRECTORY(27),
    CANNOT_READ_FILE(28),
    UNIDENTIFIED_IO_ERROR_READING_FILE(29),
    FILE_DOES_NOT_EXIST(30),
    UNIDENTIFIED_IO_ERROR_DELETING_DIR(31),
    RESULT_NO_RECORD(32),
    DUPLICATE_KEY(33),
    LOAD_NON_EMPTY_INDEX(34),
    MODIFY_NOT_SUPPORTED_IN_EXTERNAL_INDEX(35),
    FLUSH_NOT_SUPPORTED_IN_EXTERNAL_INDEX(36),
    UPDATE_OR_DELETE_NON_EXISTENT_KEY(37),
    INDEX_NOT_UPDATABLE(38),
    OCCURRENCE_THRESHOLD_PANIC_EXCEPTION(39),
    UNKNOWN_INVERTED_INDEX_TYPE(40),
    CANNOT_PROPOSE_LINEARIZER_DIFF_DIMENSIONS(41),
    CANNOT_PROPOSE_LINEARIZER_FOR_TYPE(42),
    RECORD_IS_TOO_LARGE(43),
    FAILED_TO_RE_FIND_PARENT(44),
    FAILED_TO_FIND_TUPLE(45),
    UNSORTED_LOAD_INPUT(46),
    OPERATION_EXCEEDED_MAX_RESTARTS(47),
    DUPLICATE_LOAD_INPUT(48),
    CANNOT_CREATE_ACTIVE_INDEX(49),
    CANNOT_ACTIVATE_ACTIVE_INDEX(50),
    CANNOT_DEACTIVATE_INACTIVE_INDEX(51),
    CANNOT_DESTROY_ACTIVE_INDEX(52),
    CANNOT_CLEAR_INACTIVE_INDEX(53),
    CANNOT_ALLOCATE_MEMORY_FOR_INACTIVE_INDEX(54),
    RESOURCE_DOES_NOT_EXIST(55),
    DISK_COMPONENT_SCAN_NOT_ALLOWED_FOR_SECONDARY_INDEX(56),
    CANNOT_FIND_MATTER_TUPLE_FOR_ANTI_MATTER_TUPLE(57),
    TASK_ABORTED(58),
    OPEN_ON_OPEN_WRITER(59),
    OPEN_ON_FAILED_WRITER(60),
    NEXT_FRAME_ON_FAILED_WRITER(61),
    NEXT_FRAME_ON_CLOSED_WRITER(62),
    FLUSH_ON_FAILED_WRITER(63),
    FLUSH_ON_CLOSED_WRITER(64),
    FAIL_ON_FAILED_WRITER(65),
    MISSED_FAIL_CALL(66),
    CANNOT_CREATE_FILE(67),
    NO_MAPPING_FOR_FILE_ID(68),
    NO_MAPPING_FOR_FILENAME(69),
    CANNOT_GET_NUMBER_OF_ELEMENT_FROM_INACTIVE_FILTER(70),
    CANNOT_CREATE_BLOOM_FILTER_BUILDER_FOR_INACTIVE_FILTER(71),
    CANNOT_CREATE_BLOOM_FILTER_WITH_NUMBER_OF_PAGES(72),
    CANNOT_ADD_TUPLES_TO_DUMMY_BLOOM_FILTER(73),
    CANNOT_CREATE_ACTIVE_BLOOM_FILTER(74),
    CANNOT_DEACTIVATE_INACTIVE_BLOOM_FILTER(75),
    CANNOT_DESTROY_ACTIVE_BLOOM_FILTER(76),
    CANNOT_PURGE_ACTIVE_INDEX(77),
    CANNOT_PURGE_ACTIVE_BLOOM_FILTER(78),
    CANNOT_BULK_LOAD_NON_EMPTY_TREE(79),
    CANNOT_CREATE_EXISTING_INDEX(80),
    FILE_ALREADY_MAPPED(81),
    FILE_ALREADY_EXISTS(82),
    NO_INDEX_FOUND_WITH_RESOURCE_ID(83),
    FOUND_OVERLAPPING_LSM_FILES(84),
    FOUND_MULTIPLE_TRANSACTIONS(85),
    UNRECOGNIZED_INDEX_COMPONENT_FILE(86),
    UNEQUAL_NUM_FILTERS_TREES(87),
    INDEX_NOT_MODIFIABLE(88),
    GROUP_BY_MEMORY_BUDGET_EXCEEDS(89),
    ILLEGAL_MEMORY_BUDGET(90),
    TIMEOUT(91),
    JOB_HAS_BEEN_CLEARED_FROM_HISTORY(92),
    FAILED_TO_READ_RESULT(93),
    CANNOT_READ_CLOSED_FILE(94),
    TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME(95),
    ILLEGAL_ATTEMPT_TO_ENTER_EMPTY_COMPONENT(96),
    ILLEGAL_ATTEMPT_TO_EXIT_EMPTY_COMPONENT(97),
    A_FLUSH_OPERATION_HAS_FAILED(98),
    A_MERGE_OPERATION_HAS_FAILED(99),
    FAILED_TO_SHUTDOWN_EVENT_PROCESSOR(100),
    PAGE_DOES_NOT_EXIST_IN_FILE(101),
    VBC_ALREADY_OPEN(102),
    VBC_ALREADY_CLOSED(103),
    INDEX_DOES_NOT_EXIST(104),
    CANNOT_DROP_IN_USE_INDEX(105),
    CANNOT_DEACTIVATE_PINNED_BLOOM_FILTER(106),
    PREDICATE_CANNOT_BE_NULL(107),
    FULLTEXT_ONLY_EXECUTABLE_FOR_STRING_OR_LIST(108),
    NOT_ENOUGH_BUDGET_FOR_TEXTSEARCH(109),
    CANNOT_CONTINUE_TEXT_SEARCH_HYRACKS_TASK_IS_NULL(110),
    CANNOT_CONTINUE_TEXT_SEARCH_BUFFER_MANAGER_IS_NULL(111),
    CANNOT_ADD_ELEMENT_TO_INVERTED_INDEX_SEARCH_RESULT(112),
    UNDEFINED_INVERTED_LIST_MERGE_TYPE(113),
    NODE_IS_NOT_ACTIVE(114),
    LOCAL_NETWORK_ERROR(115),
    ONE_TUPLE_RANGEMAP_EXPECTED(116),
    NO_RANGEMAP_PRODUCED(117),
    RANGEMAP_NOT_FOUND(118),
    UNSUPPORTED_WINDOW_SPEC(119),
    EOF(120),
    NUMERIC_PROMOTION_ERROR(121),
    ERROR_PRINTING_PLAN(122),
    INSUFFICIENT_MEMORY(123),
    PARSING_ERROR(124),
    INVALID_INVERTED_LIST_TYPE_TRAITS(125),
    ILLEGAL_STATE(126),
    CANNOT_SEND_PRIMARY_INDEX_STATISTICS(127),
    STATISTICS_ALLOWED_ONLY_FOR_BTREE(128),

    // Compilation error codes.
    RULECOLLECTION_NOT_INSTANCE_OF_LIST(10000),
    CANNOT_COMPOSE_PART_CONSTRAINTS(10001),
    PHYS_OPERATOR_NOT_SET(10002),
    DESCRIPTOR_GENERATION_ERROR(10003),
    EXPR_NOT_NORMALIZED(10004),
    OPERATOR_NOT_IMPLEMENTED(10005),
    INAPPLICABLE_HINT(10006),
    CROSS_PRODUCT_JOIN(10007),
    GROUP_ALL_DECOR(10008);

    private static final String RESOURCE_PATH = "errormsg/en.properties";
    public static final String HYRACKS = "HYR";
    private final int intValue;

    ErrorCode(int intValue) {
        this.intValue = intValue;
    }

    @Override
    public String component() {
        return HYRACKS;
    }

    @Override
    public int intValue() {
        return intValue;
    }

    @Override
    public String errorMessage() {
        return ErrorMessageMapHolder.get(this);
    }

    private static class ErrorMessageMapHolder {
        private static final String[] enumMessages =
                ErrorMessageUtil.defineMessageEnumOrdinalMap(values(), RESOURCE_PATH);

        private static String get(ErrorCode errorCode) {
            return enumMessages[errorCode.ordinal()];
        }
    }
}
-module(transaction_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("../include/emysql.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2,
         successful_transaction/1, failing_transaction/1]).

-define(pool_name, ?MODULE).

all() -> 
    [successful_transaction, failing_transaction].

init_per_suite(Config) ->
    crypto:start(),
    application:start(emysql),
    emysql:add_pool(?pool_name, [
        {size, 1},
        {user, test_helper:test_u()},
        {password, test_helper:test_p()},
        {host, "localhost"},
        {port, 3306},
        {database, "hello_database"},
        {encoding, utf8},
        {warnings, true}]),
    Config.

end_per_suite(_Config) ->
	emysql:remove_pool(?pool_name),
	ok.

init_per_testcase(_TestCase, Config) ->
    emysql:execute(?pool_name, <<"CREATE TABLE transaction_test (foo INT) ENGINE=InnoDB">>),
    Config.

end_per_testcase(_TestCase, _Config) ->
    emysql:execute(?pool_name, <<"DROP TABLE transaction_test">>),
    ok.

successful_transaction(_Config) ->
    emysql:transaction(?pool_name, fun (Connection) ->
        emysql_conn:execute(Connection, <<"INSERT INTO transaction_test (foo) VALUES (42)">>, []),
        emysql_conn:execute(Connection, <<"INSERT INTO transaction_test (foo) VALUES (43)">>, [])
    end),
    %% Check that these rows were inserted.
    #result_packet{rows = [[42], [43]]} =
        emysql:execute(?pool_name, <<"SELECT foo FROM transaction_test ORDER BY foo">>),
    ok.

failing_transaction(_Config) ->
    catch emysql:transaction(?pool_name, fun (Connection) ->
        emysql_conn:execute(Connection, <<"INSERT INTO transaction_test (foo) VALUES (42)">>, []),
        emysql_conn:execute(Connection, <<"INSERT INTO transaction_test (foo) VALUES (43)">>, []),
        error(foo)
    end),
    %% Check that no rows were inserted.
    Result = emysql:execute(?pool_name, <<"SELECT foo FROM transaction_test">>),
    #result_packet{rows = []} = Result,
    ok.

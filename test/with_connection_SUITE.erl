-module(with_connection_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("../include/emysql.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         concurrent_connections/1, error_in_job/1]).

-define(pool_name, ?MODULE).
-define(pool_size, 2).

all() -> 
    [concurrent_connections, error_in_job].

init_per_suite(Config) ->
    crypto:start(),
    application:start(emysql),
    emysql:add_pool(?pool_name, [
        {size, ?pool_size},
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

%% Work on two different connections simultaneously. We set a variable, then
%% fetch it again.
%%--------------------------------------------------------------------
concurrent_connections(_Config) ->
    % Clean-up before, just in case (looks like bad practice)
    %catch emysql:remove_pool(pool1) end,

    ParentPid = self(),

    %% Assert that all connections are available before we start.
    ?pool_size = num_available_connections(),

    %% Start two jobs that set a variable and fetches the variable again in a connection.
    %% Syncronizes with the main process a few times.
    Pid1 = spawn_link(fun () ->
        emysql:with_connection(?pool_name, fun (Connection) ->
            ParentPid ! {job1, started},
            #ok_packet{} = emysql_conn:execute(Connection, <<"SET @foo = 42">>, []),
            receive continue -> ok end,
            #result_packet{rows = [[42]]} =
                emysql_conn:execute(Connection, <<"SELECT @foo">>, [])
        end),
        ParentPid ! {job1, done}
    end),
    Pid2 = spawn_link(fun () ->
        emysql:with_connection(?pool_name, fun (Connection) ->
            ParentPid ! {job2, started},
            #ok_packet{} = emysql_conn:execute(Connection, <<"SET @foo = 'bar'">>, []),
            receive continue -> ok end,
            #result_packet{rows = [[<<"bar">>]]} =
                emysql_conn:execute(Connection, <<"SELECT @foo">>, [])
        end),
        ParentPid ! {job2, done}
    end),

    %% Wait for them to start.
    receive {job1, started} -> ok end,
    receive {job2, started} -> ok end,

    %% Assert that we have 2 fewer connections available in the pool.
    ?pool_size = num_available_connections() + 2,

    %% Tell them to continue and finish the work.
    Pid1 ! continue,
    Pid2 ! continue,

    %% Wait for them to finish
    receive {job1, done} -> ok end,
    receive {job2, done} -> ok end,

    %% Assert that all connections are back in the pool again.
    ?pool_size = num_available_connections(),

    ok.

%% Make sure the number of available connections in the pool is unaffected by an error in
%% a job.
error_in_job(_Config) ->
    %% Assert that all connections are available before we start.
    ?pool_size = num_available_connections(),

    try
        emysql:with_connection(
            ?pool_name,
            fun (_Connection) ->
                throw(foo)
            end
        ),
        error(never_reached)
    catch
        throw:foo -> ok
    end,

    %% Assert that all connections are back in the pool again.
    ?pool_size = num_available_connections(),

    ok.

%% --- Internal ---

%% Returns the number of available connections in our pool. Used in the tests.
num_available_connections() ->
    {Pool, _} = emysql_conn_mgr:find_pool(?pool_name, emysql_conn_mgr:pools()),
    queue:len(Pool#pool.available).

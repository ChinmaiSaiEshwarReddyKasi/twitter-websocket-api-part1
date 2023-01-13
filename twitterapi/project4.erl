-module(project4).
-import(io, [fwrite/1, fwrite/2, format/2]).
-export([test/1, generateUsers/3, server/0, client/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Client %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

client(ServerId) ->
    receive
        {createProfile, UserData} ->
            ServerId ! {register, UserData},
            client(ServerId);

        {subscribeToUser, SubscriberUsername, SubscriptionUsername} ->
            ServerId ! {subscribe, SubscriberUsername, SubscriptionUsername},
            client(ServerId);

        {tweet, Username, Tweet} ->
            ServerId ! {createTweet, Username, Tweet},
            client(ServerId);

        {updateMention, Username} ->
            format("I ~p got a new mention~n", [Username]),
            client(ServerId);

        {updateFeed, Username, Tweet} ->
            ServerId ! {updateUserFeed, Username, Tweet},
            format("~p - My feed has been updated~n", [Username]),
            client(ServerId);

        {reTweet, Username, Tweet} ->
            ServerId ! {handleReTweet, Username, Tweet},
            format("~p~n", [Tweet]),
            client(ServerId);

        {getTweets, Username} ->
            format("getting my tweets ~p~n", [Username]),
            ServerId ! {getMyTweets, Username},
            client(ServerId);

        {receiveMyTweets, Username, MyTweets} ->
            format("~p: My Tweets are - ~p~n", [Username, MyTweets]),
            twitter ! {tweetsQuery},
            client(ServerId);

        {getSubscribedTweets, Username} ->
            format("getting my subscribed tweets ~p~n", [Username]),
            ServerId ! {getMySubscribedTweets, Username},
            client(ServerId);

        {receiveMySubscribedTweets, Username, MyTweets} ->
            format("~p: My subscribed Tweets are - ~p~n", [Username, MyTweets]),
            twitter ! {subscribedTweetsQuery},
            client(ServerId);

        {getMyMentions, Username} ->
            format("getting my mentions ~p~n", [Username]),
            ServerId ! {getMyMentions, Username},
            client(ServerId);

        {receiveMyMentions, Username, Tweets} ->
            format("~p: My mentioned tweets are - ~p~n", [Username, Tweets]),
            twitter ! {mentionsQuery},
            client(ServerId);

        {searchHastag, Username, Hashtag} ->
            format("Searching for tweets with hastag ~p~n", [Hashtag]),
            ServerId ! {searchHastagTweets, Username, Hashtag},
            client(ServerId);

        {receiveHashtagTweets, Hashtag, Tweets} ->
            format("Results for hashtag ~p are: ~p~n", [Hashtag, Tweets]),
            twitter ! {hastagQuery},
            client(ServerId);

        {disconnect, Username} ->
            format("~p is now disconnecting~n", [Username]),
            ServerId ! {disconnectUser, Username}
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Server %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

server() -> 
    receive
        {register, UserData} ->
            insertIntoUserTable(maps:get("Username", UserData), UserData),
            server();

        {subscribe, SubscriberUsername, SubscriptionUsername} ->
            SubscriberUserData = getUserData(SubscriberUsername),
            SubscriptionUserData = getUserData(SubscriptionUsername),

            Data1 = maps:update("Subscribers", maps:get("Subscribers", SubscriptionUserData) ++ [SubscriberUsername], SubscriptionUserData),
            Data2 = maps:update("Subscriptions", maps:get("Subscriptions", SubscriberUserData) ++ [SubscriptionUsername], SubscriberUserData),
            
            insertIntoUserTable(SubscriberUsername, Data2),
            insertIntoUserTable(SubscriptionUsername, Data1),

            % format("Subscriber Data: ~p~n Subscription Data: ~p~n", [ets:lookup(userTable, list_to_atom(SubscriberUsername)), ets:lookup(userTable, list_to_atom(SubscriptionUsername))]),
            server();

        {createTweet, Username, Tweet} ->
            UserData = getUserData(Username),
            HasMention = lists:member(hd("@"), Tweet),
            HasHastag = lists:member(hd("#"), Tweet),

            UpdatedTweets = maps:update("Tweets", maps:get("Tweets", UserData) ++ [Tweet], UserData),
            insertIntoUserTable(Username, UpdatedTweets),

            SubscribersData = maps:get("Subscribers", UpdatedTweets),
            lists:foreach(
                fun(Uname) ->
                    FeedData = getUserData(Uname),
                    UserPid = maps:get("UserPid", FeedData),

                    if(UserPid =:= undefined) ->
                        format("Server: User with username ~p is disconnected!! Live updates about the tweet cannot be provided.~n", [Uname]),
                        UpdatedFeedData = maps:update("Feed", maps:get("Feed", FeedData) ++ [Tweet], FeedData),
                        insertIntoUserTable(Uname, UpdatedFeedData);
                    true ->
                        UpdatedFeedData = maps:update("Feed", maps:get("Feed", FeedData) ++ [Tweet], FeedData),
                        insertIntoUserTable(Uname, UpdatedFeedData),
                        format("~p - My feed has been updated~n", [list_to_atom(Uname)])
                    end
                    % timer:sleep(5)
                end,
                SubscribersData
            ),

            if(HasMention) ->
                MentionUsername = string:substr(lists:nth(1, string:split(string:find(Tweet, "@"), " ")), 2),
                UpdatedData = maps:update("Mentions", maps:get("Mentions", getUserData(MentionUsername)) ++ [Tweet], getUserData(MentionUsername)),

                insertIntoUserTable(MentionUsername, UpdatedData),
                notifyUserMention(MentionUsername);
            true ->
                ok
            end,

            if(HasHastag) ->
                Hashtag = lists:nth(1, string:split(string:find(Tweet, "#"), " ")),
                insertIntoHastagTable(Tweet, Hashtag);
            true ->
                ok
            end,
            twitter ! {tweeted},
            server();

        {updateUserFeed, Username, Tweet} ->
            UserData = getUserData(Username),
            UpdatedData = maps:update("Feed", maps:get("Feed", UserData) ++ [Tweet], UserData),
            insertIntoUserTable(Username, UpdatedData),
            server();
                
        {handleReTweet, Username, Tweet} ->
            UserData = getUserData(Username),

            UpdatedTweets = maps:update("Tweets", maps:get("Tweets", UserData) ++ [Tweet], UserData),
            insertIntoUserTable(Username, UpdatedTweets),

            SubscribersData = maps:get("Subscribers", UpdatedTweets),
            lists:foreach(
                fun(Uname) ->
                    Pid = maps:get("UserPid", getUserData(Uname)),
                    Pid ! {updateFeed, Uname, Tweet},
                    timer:sleep(5)
                end,
                SubscribersData
            ),
            twitter ! {reTweeted},
            server();

        {getMyTweets, Username} ->
            UserData = getUserData(Username),
            MyTweets = maps:get("Tweets", UserData),
            Pid = maps:get("UserPid", UserData),
            Pid ! {receiveMyTweets, Username, MyTweets},
            server();

        {getMySubscribedTweets, Username} ->
            UserData = getUserData(Username),
            MySubscribedTweets = maps:get("Feed", UserData),
            Pid = maps:get("UserPid", UserData),
            Pid ! {receiveMySubscribedTweets, Username, MySubscribedTweets},
            server();

        {getMyMentions, Username} ->
            UserData = getUserData(Username),
            MyTweets = maps:get("Mentions", UserData),
            Pid = maps:get("UserPid", UserData),
            Pid ! {receiveMyMentions, Username, MyTweets},
            server();

        {searchHastagTweets, Username, Hashtag} ->
            UserData = getUserData(Username),
            Pid = maps:get("UserPid", UserData),
            HashtagData = getHashtagData(Hashtag),
            Pid ! {receiveHashtagTweets, Hashtag, HashtagData},
            server();

        {disconnectUser, Username} ->
            UserData = getUserData(Username),
            UpdatedData = maps:update("UserPid", undefined, UserData),
            insertIntoUserTable(Username, UpdatedData),
            server()            

    end.

notifyUserMention(Username) ->
    Pid = maps:get("UserPid", getUserData(Username)),
    Pid ! {updateMention, Username}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Test Suite %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test(NumClients) ->
    register(twitter, self()),
    checkTableStatus(),

    ets:new(hashtagTable, [set, named_table, public]),
    ets:new(userTable, [set, named_table, public]),

    ServerPid = startServer(),

    
    statistics(wall_clock),
    generateUsers(1, NumClients, ServerPid),
    {_, T1} = statistics(wall_clock),

    subscribeUsers(1, NumClients),
    {_, T2} = statistics(wall_clock),

    generateTweets(1, NumClients, 0),
    timer:sleep(1000),

    generateReTweets(1, NumClients),
    {_, T3} = statistics(wall_clock),

    getSubscribedTweets(1, NumClients),
    {_, T4} = statistics(wall_clock),

    getMyTweets(1, NumClients),
    {_, T5} = statistics(wall_clock),

    getMyMentions(1, NumClients),
    {_, T6} = statistics(wall_clock),

    getHashtagTweets("#COP5615isgreat"),
    {_, T7} = statistics(wall_clock),

    disconnectUsers(NumClients, ServerPid),
    {_, T8} = statistics(wall_clock),


    format("Average time to generate users: ~pms~n", [T1/NumClients]),
    format("Average time to Zipf subscribe to users per client: ~pms~n", [T2/NumClients]),
    format("Average time to tweet and re-tweet per client: ~pms~n", [T3/NumClients]),
    format("Average time to query subscribed tweets per client: ~pms~n", [T4/NumClients]),
    format("Average time to query all tweets per client: ~pms~n", [T5/NumClients]),
    format("Average time to query all mentioned tweets per client: ~pms~n", [T6/NumClients]),
    format("Average time to query hastag: ~pms~n", [T7/NumClients]),
    format("Average time to tweet when some users are disconnected: ~pms~n", [T8]),
    format("Total Time ~p~n", [T1+T2+T3+T4+T5+T6+T7+T8]),

    unregister(twitter).

startServer() ->
    Pid = spawn(project4, server, []),
    Pid.

generateUsers(Index, NumClients, _) when Index > NumClients ->
    ok;
generateUsers(Index, NumClients, ServerPid) ->
    format("Generating User ~p~n", [Index]),
    UserName = generateUsername(Index),
    Email = UserName ++ "@gmail.com",
    Password = "Password@123",
    Pid = spawn(project4, client, [ServerPid]),
    UserData = generateProfile(Email, UserName, Password, Pid),
    % format("User Data: ~p~n", [UserData]),
    Pid ! {createProfile, UserData},
    timer:sleep(5),
    generateUsers(Index+1, NumClients, ServerPid).

generateProfile(Email, UserName, Password, Pid) ->
    UserData = #{
        "Email" => Email,
        "Username" => UserName,
        "Password" => Password,
        "Tweets" => [],
        "Subscribers" => [],
        "Subscriptions" => [],
        "Feed" => [],
        "Mentions" => [],
        "UserPid" => Pid
    },
    UserData.

subscribeUsers(Index, NumClients) when Index > NumClients ->
    ok;
subscribeUsers(Index, NumClients) ->
    format("Zipf subscribing for User ~p~n", [Index]),
    Username = generateUsername(Index),
    NumSubscribers = handleZipfDistribution(NumClients, Index),
    SubList = generateSubscriptions(1, NumSubscribers, []),

    Pid = maps:get("UserPid", getUserData(Username)),
    lists:foreach(
        fun(SubPid) ->
            if(Username /= SubPid) ->
                Pid ! {subscribeToUser, Username, SubPid},
                timer:sleep(5);
            true ->
                ok
            end
        end,
        SubList
    ),
    subscribeUsers(Index+1, NumClients).
    

generateSubscriptions(Index, NumSubRequests, SubList) when Index > NumSubRequests ->
    SubList;
generateSubscriptions(Index, NumSubRequests, SubList) ->
    generateSubscriptions(Index+1, NumSubRequests, SubList ++ [generateUsername(Index)]).

generateTweets(Index, NumClients, ConvergenceValue) when Index > NumClients ->
    convergenceForTweets(ConvergenceValue);
generateTweets(Index, NumClients, ConvergenceValue) ->
    Username = generateUsername(Index),
    NumTweets = handleZipfDistribution(NumClients, Index),
    Pid = maps:get("UserPid", getUserData(Username)),

    % Mention Tweet
    RandomUser = generateUsername(rand:uniform(NumClients)),
    MentionTweet = Username ++ " is tweeting a mention with @" ++ RandomUser,
    Pid ! {tweet, Username, MentionTweet},

    % Hastag Tweet
    HastagTweet = Username ++ " is tweeting with a hastag #COP5615isgreat",
    Pid ! {tweet, Username, HastagTweet},

    % Tweets
    lists:foreach(
        fun(TweetCount) ->
            FinalTweet = Username ++ " is tweeting their " ++ integer_to_list(TweetCount) ++ " tweet.",
            Pid ! {tweet, Username, FinalTweet}
        end,
        lists:seq(1, NumTweets)
    ),
    generateTweets(Index+1, NumClients, ConvergenceValue + NumTweets + 2).

convergenceForTweets(0) ->
    ok;
convergenceForTweets(NumClients) ->
    receive
        {tweeted} -> 
            % format("Remaining Clients ~p~n", [NumClients -1]),
            convergenceForTweets(NumClients - 1)
    end.

generateReTweets(Index, NumClients) when Index > NumClients ->
    convergenceForReTweets(NumClients);
generateReTweets(Index, NumClients) ->
    Username = generateUsername(Index),
    UserData = getUserData(Username),
    Pid = maps:get("UserPid", UserData),
    SubscribedTweets = maps:get("Feed", UserData),
    ReTweet = Username ++ " re-tweeted the tweet: " ++ lists:nth(rand:uniform(length(SubscribedTweets)), SubscribedTweets),
    Pid ! {reTweet, Username, ReTweet},
    generateReTweets(Index+1, NumClients).

convergenceForReTweets(0) ->
    ok;
convergenceForReTweets(NumClients) ->
    receive
        {reTweeted} ->
            convergenceForReTweets(NumClients-1)
    end.

getSubscribedTweets(Index, NumClients) when Index > NumClients ->
    convergenceForSubscribedTweetsQuery(NumClients);
getSubscribedTweets(Index, NumClients) ->
    Username = generateUsername(Index),
    UserData = getUserData(Username),
    Pid = maps:get("UserPid", UserData),

    Pid ! {getSubscribedTweets, Username},
    getSubscribedTweets(Index+1, NumClients).

convergenceForSubscribedTweetsQuery(0) ->
    ok;
convergenceForSubscribedTweetsQuery(NumClients) ->
    receive
        {subscribedTweetsQuery} ->
            convergenceForSubscribedTweetsQuery(NumClients-1)
    end.

getMyTweets(Index, NumClients) when Index > NumClients ->
    convergenceForTweetsQuery(NumClients);
getMyTweets(Index, NumClients) ->
    Username = generateUsername(Index),
    UserData = getUserData(Username),
    Pid = maps:get("UserPid", UserData),

    Pid ! {getTweets, Username},
    getMyTweets(Index+1, NumClients).

convergenceForTweetsQuery(0) ->
    ok;
convergenceForTweetsQuery(NumClients) ->
    receive
        {tweetsQuery} ->
            convergenceForTweetsQuery(NumClients-1)
    end.

getMyMentions(Index, NumClients) when Index > NumClients ->
    convergenceForMentions(NumClients);
getMyMentions(Index, NumClients) ->
    Username = generateUsername(Index),
    UserData = getUserData(Username),
    Pid = maps:get("UserPid", UserData),

    Pid ! {getMyMentions, Username},
    getMyMentions(Index+1, NumClients).

convergenceForMentions(0) ->
    ok;
convergenceForMentions(NumClients) ->
    receive
        {mentionsQuery} ->
            convergenceForMentions(NumClients-1)
    end.

getHashtagTweets(Hashtag) ->
    Username = generateUsername(1),
    UserData = getUserData(Username),
    Pid = maps:get("UserPid", UserData),

    Pid ! {searchHastag, Username, Hashtag},

    receive
        {hastagQuery} -> ok
    end.

disconnectUsers(NumClients, ServerPid) ->
    List = handleDisconnection(NumClients, 5, 0, []),
    Subscriptions = maps:get("Subscriptions", getUserData(lists:nth(rand:uniform(length(List)), List))),
    ServerPid ! {createTweet, lists:nth(1, Subscriptions), "This is a new tweet"},
    receive
        {tweeted} -> ok
    end.

handleDisconnection(NumClients, ClientsToDisconnect, CLientsLeftToDisconnect, DisconnectedList) ->
    if (CLientsLeftToDisconnect < ClientsToDisconnect) ->
        RandomUser = rand:uniform(NumClients),
        RandomUsername = generateUsername(RandomUser),
        RandomUserId = maps:get("UserPid", getUserData(RandomUsername)),
        if(RandomUserId /= undefined) ->
            NewDisconnectedList = DisconnectedList ++ [RandomUsername],
            RandomUserId ! {disconnect, RandomUsername},
            timer:sleep(10),
            exit(RandomUserId, kill),
            handleDisconnection(NumClients, ClientsToDisconnect, CLientsLeftToDisconnect+1, NewDisconnectedList);
        true ->
            handleDisconnection(NumClients, ClientsToDisconnect, CLientsLeftToDisconnect, DisconnectedList)
        end;
    true ->
        DisconnectedList
    end.
        



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Util functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

checkTableStatus() ->
    ServerTable = ets:whereis(hashtagTable),
    UserTable = ets:whereis(userTable),

    if(ServerTable /= undefined) ->
        ets:delete(hashtagTable);
    true -> 
        ok
    end,

    if(UserTable /= undefined) ->
        ets:delete(userTable);
    true -> 
        ok
    end.

generateUsername(Index) ->
    Username = "User_" ++ integer_to_list(Index),
    Username.

getUserData(Username) ->
    element(2, 
        lists:nth(1, 
            ets:lookup(userTable, list_to_atom(Username))
        )
    ).

insertIntoUserTable(Username, Data) ->
    ets:insert(userTable, {list_to_atom(Username), Data}).

insertIntoHastagTable(Tweet, Hashtag) ->
    ets:insert(hashtagTable, {list_to_atom(Hashtag), getHashtagData(Hashtag) ++ [Tweet]}).

getHashtagData(Hashtag) ->
    Data = ets:lookup(hashtagTable, list_to_atom(Hashtag)),
    if(Data /= []) ->
        Result = element(2, lists:nth(1, Data));
    true ->
        Result = []
    end,
    Result.

handleZipfDistribution(NumClients, Index) ->
    Value = round(math:floor(NumClients/(5*Index))),
    if(Value =< 0) ->
        Result = 1;
    true ->
        Result = Value
    end,
    Result.

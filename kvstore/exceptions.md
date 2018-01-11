780
[remote]b03-08-36251 execute command : Put{-1147912689,520639236} in node b03-08-24470
b03-08-24470 recive command : Put{-1147912689,520639236} from node b03-08-36251
[local]b03-08-24470 execute command : Put{-1147912689,520639236} in node b03-08-24470
b03-08-36251 recive command : Reply{-1147912689,null} from node b03-08-24470
[local]b03-08-24470 execute command : Get{-1147912689} in node b03-08-24470
b03-08-24470 get result and return :520639236
781
[remote]b03-08-36251 execute command : Put{-989313190,929754250} in node b03-08-24470
[local]b03-08-24470 execute command : Get{-989313190} in node b03-08-24470
b03-08-24470 get result and return :null
b03-08-24470 recive command : Put{-989313190,929754250} from node b03-08-36251
[local]b03-08-24470 execute command : Put{-989313190,929754250} in node b03-08-24470
b03-08-36251 recive command : Reply{-989313190,null} from node b03-08-24470

java.lang.NullPointerException
	local get command recive before remote put


28
[remote]b03-08-27035 execute command : Put{-703869892,800150819} in node b03-08-27308
b03-08-27308 recive command : Put{-703869892,800150819} from node b03-08-27035
[local]b03-08-27308 execute command : Put{-703869892,800150819} in node b03-08-27308
b03-08-27035 recive command : Reply{-703869892,null} from node b03-08-27308
[remote]b03-08-56804 execute command : Get{-703869892} in node b03-08-27308
b03-08-27308 recive command : Get{-703869892} from node b03-08-56804
[local]b03-08-27308 execute command : Get{-703869892} in node b03-08-27308
b03-08-27308 get result and return :800150819
b03-08-56804 recive command : Reply{-703869892,800150819} from node b03-08-27308
b03-08-56804 get result and return :800150819

32
[remote]b03-08-27035 execute command : Put{614008627,-1347031112} in node b03-08-56804
b03-08-56804 recive command : Put{614008627,-1347031112} from node b03-08-27035
[local]b03-08-56804 execute command : Put{614008627,-1347031112} in node b03-08-56804
b03-08-27035 recive command : Reply{614008627,null} from node b03-08-56804
[remote]b03-08-27308 execute command : Get{614008627} in node b03-08-56804
b03-08-56804 recive command : Get{614008627} from node b03-08-27308
[local]b03-08-56804 execute command : Get{614008627} in node b03-08-56804
b03-08-56804 get result and return :-1347031112
b03-08-27308 recive command : Reply{614008627,-1347031112} from node b03-08-56804
b03-08-27308 get result and return :-1347031112

35
[remote]b03-08-27035 execute command : Put{173502410,201182988} in node b03-08-56804
b03-08-56804 recive command : Put{173502410,201182988} from node b03-08-27035
[local]b03-08-56804 execute command : Put{173502410,201182988} in node b03-08-56804
b03-08-27035 recive command : Reply{173502410,null} from node b03-08-56804
[remote]b03-08-27308 execute command : Get{173502410} in node b03-08-56804
b03-08-27308 get result and return :-1347031112
b03-08-56804 recive command : Get{173502410} from node b03-08-27308
[local]b03-08-56804 execute command : Get{173502410} in node b03-08-56804
b03-08-56804 get result and return :201182988
b03-08-27308 recive command : Reply{173502410,201182988} from node b03-08-56804

java.lang.AssertionError
	remote put and remote get

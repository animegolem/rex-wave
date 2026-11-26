---- MODULE MCMailStore ----
EXTENDS MailStore
MC_pathOf == [m \in Msgs |-> IF m = "b" THEN "p2" ELSE "p1"]
MC_Edits == {<<"a", "a2">>}
====

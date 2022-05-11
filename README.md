# **Borealis Banhammer**

Borealis Banhammer is the ban management daemon for misbehaving unfair users.

It's an events listener for Aurora's transactions' events (sourced from nginx based relayer interceptor and published to the Borealis NATS Bus).

Provides publishing (as producer) of messages about banning events with streaming messages to the Borealis Bus, NATS based service-oriented bus (MOM/MQ), for other security/policy services (as consumers/subscribers).

## **Build and run Borealis Banhammer using make.sh shell helper and Cargo:**
```
bash ./make.sh [ help/h/? | fmt | check | build | build release | submodules | submodules update | exec | exec_logging | exec_logging_cliout ]
```

```
bash ./make.sh fmt

bash ./make.sh check

bash ./make.sh submodules update

bash ./make.sh submodules

bash ./make.sh build release

bash ./make.sh build
```

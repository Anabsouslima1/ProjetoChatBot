package chat.java;

import chat.proto.*;

// Texto legível do conteúdo de uma mensagem para os logs, no MESMO formato do C++
// (cpp/src/common.hpp, função describe), para que o scripts/check_parte4.py leia os logs
// de todas as linguagens. Ex.: coordinator_announcement{name=server_cpp_2}
final class Descricao {
    private Descricao() {
    }

    private static String status(boolean ok, ErrorCode code, String msg) {
        return "success=" + ok + (ok ? "" : ", erro=" + code.name() + " (" + msg + ")");
    }

    static String de(Envelope e) {
        switch (e.getBodyCase()) {
            case LOGIN_REQUEST:
                return "login_request{username=" + e.getLoginRequest().getUsername() + "}";
            case LOGIN_RESPONSE: {
                LoginResponse r = e.getLoginResponse();
                return "login_response{" + status(r.getSuccess(), r.getError(), r.getErrorMessage()) + "}";
            }
            case CREATE_CHANNEL_REQUEST:
                return "create_channel_request{channel_name=" + e.getCreateChannelRequest().getChannelName() + "}";
            case CREATE_CHANNEL_RESPONSE: {
                CreateChannelResponse r = e.getCreateChannelResponse();
                return "create_channel_response{" + status(r.getSuccess(), r.getError(), r.getErrorMessage()) + "}";
            }
            case LIST_CHANNELS_REQUEST:
                return "list_channels_request{}";
            case LIST_CHANNELS_RESPONSE: {
                StringBuilder sb = new StringBuilder("list_channels_response{channels=[");
                boolean primeiro = true;
                for (String c : e.getListChannelsResponse().getChannelsList()) {
                    if (!primeiro) sb.append(", ");
                    sb.append(c);
                    primeiro = false;
                }
                return sb.append("]}").toString();
            }
            case PUBLISH_REQUEST:
                return "publish_request{channel=" + e.getPublishRequest().getChannel()
                        + ", message=\"" + e.getPublishRequest().getMessage() + "\"}";
            case PUBLISH_RESPONSE: {
                PublishResponse r = e.getPublishResponse();
                return "publish_response{" + status(r.getSuccess(), r.getError(), r.getErrorMessage()) + "}";
            }
            case PUBLICATION: {
                Publication p = e.getPublication();
                return "publication{channel=" + p.getChannel() + ", author=" + p.getAuthor()
                        + ", message=\"" + p.getMessage() + "\"}";
            }
            case RANK_REQUEST:
                return "rank_request{name=" + e.getRankRequest().getName() + "}";
            case RANK_RESPONSE:
                return "rank_response{rank=" + e.getRankResponse().getRank() + "}";
            case LIST_SERVERS_REQUEST:
                return "list_servers_request{}";
            case LIST_SERVERS_RESPONSE: {
                StringBuilder sb = new StringBuilder("list_servers_response{servers=[");
                boolean primeiro = true;
                for (ServerInfo s : e.getListServersResponse().getServersList()) {
                    if (!primeiro) sb.append(", ");
                    sb.append(s.getName()).append(':').append(s.getRank());
                    primeiro = false;
                }
                return sb.append("]}").toString();
            }
            case HEARTBEAT_REQUEST:
                return "heartbeat_request{name=" + e.getHeartbeatRequest().getName() + "}";
            case HEARTBEAT_RESPONSE:
                return "heartbeat_response{success=" + e.getHeartbeatResponse().getSuccess()
                        + ", time_ms=" + e.getHeartbeatResponse().getTimeMs() + "}";
            case CLOCK_REQUEST:
                return "clock_request{}";
            case CLOCK_RESPONSE:
                return "clock_response{time_ms=" + e.getClockResponse().getTimeMs() + "}";
            case ELECTION_REQUEST:
                return "election_request{name=" + e.getElectionRequest().getName()
                        + ", rank=" + e.getElectionRequest().getRank() + "}";
            case ELECTION_RESPONSE:
                return "election_response{ok=" + e.getElectionResponse().getOk() + "}";
            case COORDINATOR_ANNOUNCEMENT:
                return "coordinator_announcement{name=" + e.getCoordinatorAnnouncement().getName() + "}";
            default:
                return "(mensagem vazia/desconhecida)";
        }
    }
}
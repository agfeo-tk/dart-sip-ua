import '../event_manager/event_manager.dart';
import '../event_manager/internal_events.dart';
import '../logger.dart';
import '../sip_message.dart';
import '../socket_transport.dart';
import '../ua.dart';
import '../utils.dart';
import 'transaction_base.dart';

class AckClientTransaction extends TransactionBase {
  /// @param queueOnFailure Bei getrenntem Transport das ACK bis zum Neuaufbau
  ///        vormerken (UA.queueAckUntilReconnect). false beim Nachsenden selbst.
  AckClientTransaction(UA ua, SocketTransport transport,
      OutgoingRequest request, EventManager eventHandlers,
      {bool queueOnFailure = true}) {
    id = 'z9hG4bK${(Math.random() * 10000000).floor()}';
    _ua = ua;
    _queueOnFailure = queueOnFailure;
    this.transport = transport;
    this.request = request;
    _eventHandlers = eventHandlers;

    String via = 'SIP/2.0/${transport.via_transport}';

    via += ' ${ua.configuration.via_host};branch=$id';

    request.setHeader('via', via);
  }

  late EventManager _eventHandlers;
  late UA _ua;
  late bool _queueOnFailure;

  /// Sendet das ACK. Ist der Transport gerade getrennt (Neuaufbau nach Netzwechsel),
  /// wird es vorgemerkt und nach dem Reconnect nachgesendet: die Gegenstelle wartet
  /// bis zu 32 s auf das ACK zu ihrem 2xx und beendet sonst das Gespraech.
  @override
  void send() {
    if (!transport!.send(request)) {
      if (_queueOnFailure && !transport!.isConnected()) {
        _ua.queueAckUntilReconnect(request as OutgoingRequest);
      }
      onTransportError();
    }
  }

  @override
  void onTransportError() {
    logger.d('transport error occurred for transaction $id');
    _eventHandlers.emit(EventOnTransportError());
  }
}

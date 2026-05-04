import { ConversationTurn } from "./ConversationTurn";

export function ConversationTimeline({ turns, latestTurnRef, timelineEndRef }) {
  return (
    <div className="thread-transcript-scroll thread-transcript-scroll--chat">
      <div className="conversation-stack thread-conversation-stack">
        {turns.map((turn, index) => (
          <ConversationTurn
            key={turn.id}
            turn={turn}
            turnRef={index === turns.length - 1 ? latestTurnRef : null}
          />
        ))}
        <div ref={timelineEndRef} />
      </div>
    </div>
  );
}

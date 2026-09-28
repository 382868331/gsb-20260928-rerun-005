import type { RobotState } from "../api";

interface Props {
  robot: RobotState | null;
  onCellClick: (x: number, y: number) => void;
}

const W = 6;
const H = 5;
const CELL = 52;

export default function Scene({ robot, onCellClick }: Props) {
  const obs = new Set((robot?.obstacles ?? []).map(([x, y]) => `${x},${y}`));
  const [px, py] = robot?.pos ?? [0, 0];
  return (
    <svg className="scene-svg" width={W * CELL} height={H * CELL}>
      {Array.from({ length: H }).map((_, y) =>
        Array.from({ length: W }).map((_, x) => {
          const blocked = obs.has(`${x},${y}`);
          return (
            <g key={`${x}-${y}`} onClick={() => onCellClick(x, y)}
               style={{ cursor: "pointer" }}>
              <rect x={x * CELL} y={y * CELL} width={CELL} height={CELL}
                    fill={blocked ? "#7f1d1d" : "#131a2b"}
                    stroke="#24304d" />
              {blocked && (
                <text x={x * CELL + CELL / 2} y={y * CELL + CELL / 2 + 5}
                      textAnchor="middle" fontSize="18" fill="#fecaca">
                  ▲
                </text>
              )}
              {x === 0 && y === 0 && !blocked && (
                <text x={x * CELL + 4} y={y * CELL + 14} fontSize="9"
                      fill="#8a97b3">基地</text>
              )}
            </g>
          );
        })
      )}
      <g style={{ transition: "transform .25s" }}
         transform={`translate(${px * CELL}, ${py * CELL})`}>
        <circle cx={CELL / 2} cy={CELL / 2} r={15}
                fill="#4f8cff" stroke="#dbeafe" strokeWidth={2} />
        <text x={CELL / 2} y={CELL / 2 + 4} textAnchor="middle"
              fontSize="11" fill="#fff">
          {robot?.carrying ? "🤖" : "R"}
        </text>
      </g>
    </svg>
  );
}

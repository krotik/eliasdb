import { BackendClient } from '../backend/api-helper';
import {
    PlayerState,
    SpriteState,
    EngineOptions,
    EngineEventHandler
} from '../display/types';
import {
    DefaultPlayerState,
    DefaultSpriteState,
    DefaultEngineEventHandler,
    DefaultEngineOptions
} from '../display/default';
import { AssetDefinition } from '../backend/asset-loader';
import { stringToNumber } from '../helper';

/**
 * Concrete implementation of the engine event handler.
 */
export class GameEventHandler
    extends DefaultEngineEventHandler
    implements EngineEventHandler {}

/**
 * Concrete implementation of the engine options.
 */
export class GameOptions extends DefaultEngineOptions implements EngineOptions {
    public eventHandler: GameEventHandler;

    constructor() {
        super();
        this.eventHandler = new GameEventHandler();
    }

    /**
     * Custom draw handler called after each draw (gets also the draw context)
     */
    public drawHandler(): void {}

    /**
     * Custom handler which gets called once the simulation has stopped
     */
    public stopHandler(): void {}
}

/**
 * Concrete implementation of the player sprite in the world.
 */
export class Player extends DefaultPlayerState implements PlayerState {
    protected gameName: string;
    protected backedClient: BackendClient;
    protected websocket: WebSocket | null = null;

    constructor(gameName: string, backedClient: BackendClient) {
        super();
        this.gameName = gameName;
        this.backedClient = backedClient;
    }

    setWebsocket(ws: WebSocket) {
        this.websocket = ws;
    }

    stateUpdate(action: string, hint?: string[]): void {
        super.stateUpdate(action, hint);

        if (this.websocket != null) {
            this.backedClient.sendSockData(this.websocket, {
                player: this.id,
                gameName: this.gameName,
                action,
                state: {
                    dir: this.dir,
                    rotSpeed: this.rotSpeed,
                    speed: this.speed,
                    strafe: this.strafe
                }
            });
        }
    }

    public draw(
        ctx: CanvasRenderingContext2D,
        state: SpriteState,
        assets: Record<string, AssetDefinition>
    ): void {
        drawPlayerSprite(ctx, state, assets);
    }
}

/**
 * Concrete implementation of a non-player sprite in the world.
 */
export class Sprite extends DefaultSpriteState implements SpriteState {
    draw(
        ctx: CanvasRenderingContext2D,
        state: SpriteState,
        assets: Record<string, AssetDefinition>
    ): void {
        if (state.kind === 'asteroid') {
            ctx.save();
            ctx.translate(state.x, state.y);
            ctx.rotate(state.rot + (Math.PI / 2) * 3);
            ctx.drawImage(
                assets['asteroid_001'].image!,
                -state.dim / 2,
                -state.dim / 2,
                state.dim,
                state.dim
            );
            ctx.restore();
            return;
        } else if (state.kind === 'shot') {
            let shotType = stringToNumber(state.owner!, 1, 3);
            let shot_dim = 38;
            ctx.save();
            ctx.translate(state.x, state.y);
            ctx.rotate(state.rot + 2 * Math.PI);
            ctx.drawImage(
                assets[`shot_00${shotType}`].image!,
                -shot_dim / 2,
                -shot_dim / 2,
                shot_dim,
                shot_dim
            );
            ctx.restore();
            return;
        } else if (state.kind === 'player') {
            drawPlayerSprite(ctx, state, assets);
            return;
        } else {
            console.log('Could not draw: ', state.kind);
        }

        throw new Error(`Method not implemented. ${ctx}, ${state}`);
    }
}

/**
 * Draw the sprite of a player.
 */
function drawPlayerSprite(
    ctx: CanvasRenderingContext2D,
    state: SpriteState,
    assets: Record<string, AssetDefinition>
) {
    let shipType = stringToNumber(state.id, 1, 9);

    ctx.save();
    ctx.translate(state.x, state.y);
    ctx.rotate(state.rot + (Math.PI / 2) * 3);

    if (state.animation) {
        state.animation.tick(
            ctx,
            -state.dim / 2,
            -state.dim / 2,
            state.dim,
            state.dim
        );
    } else {
        ctx.drawImage(
            assets[`spaceShips_00${shipType}`].image!,
            -state.dim / 2,
            -state.dim / 2,
            state.dim,
            state.dim
        );
    }

    ctx.restore();

    ctx.save();
    ctx.font = '10px Arial';
    ctx.fillStyle = '#ffbf00';

    ctx.fillText(
        state.id,
        state.x - Math.floor(ctx.measureText(state.id).width / 2),
        state.y + state.dim + 5
    );
    ctx.restore();
}

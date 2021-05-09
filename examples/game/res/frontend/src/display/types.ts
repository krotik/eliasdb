import { AssetDefinition } from '../backend/asset-loader';
import { AnimationController } from '../game/lib';

/**
 * Handler to react to player events.
 */
export interface EngineEventHandler {
    onkeydown(state: PlayerState, e: KeyboardEvent): void;
    onkeyup(state: PlayerState, e: KeyboardEvent): void;
}

/**
 * Options for the game.
 */
export interface EngineOptions {
    /**
     * Handler for player events
     */
    eventHandler: EngineEventHandler;

    /**
     * Handler called after each draw (gets also the draw context)
     */
    drawHandler(
        ctx: CanvasRenderingContext2D,
        state: PlayerState,
        sprites: SpriteState[]
    ): void;

    /**
     * Handler called once the simulation has stopped
     */
    stopHandler(): void;

    /**
     * Rendering options
     */

    backdrop: CanvasImageSource | null; // Backdrop image for game world

    // Game world dimensions

    screenWidth: number;
    screenHeight: number;

    // HTML elment dimensions (scaled if different from game world dimensions)

    screenElementWidth: number;
    screenElementHeight: number;

    // Game assets

    assets: Record<string, AssetDefinition>;
}

/**
 * State of a sprite in the world.
 */
export interface SpriteState {
    id: string; // A unique ID

    x: number; // Sprite x position
    y: number; // Sprite y position

    kind: string; // Sprint kind

    owner?: string; // Owner of this sprite

    dim: number; // Dimensions of the sprite (box)

    isMoving: boolean; // Flag if the sprite is moving or static

    // Flag if the sprite is kept in the display or if it should be
    // destroyed once it is outside of the visible area
    displayLoop: boolean;

    dir: number; // Turning direction (-1 for left, 1 for right, 0 no turning)
    rot: number; // Angle of rotation
    rotSpeed: number; // Rotation speed for each step (in radians)

    speed: number; // Moving direction (1 forward, -1 backwards, 0 no movement)
    strafe: number; // Strafing direction of sprite (-1 left, 1 right, 0 no movement)
    moveSpeed: number; // Move speed for each step

    animation: AnimationController | null; // Animation controller for this sprite

    /**
     * Set the state from a given map structure.
     */
    setState(state: Record<string, any>): void;

    /**
     * Draw this sprite.
     */
    draw(
        ctx: CanvasRenderingContext2D,
        state: SpriteState,
        assets: Record<string, AssetDefinition>
    ): void;
}

/**
 * State of the player sprite in the world.
 */
export interface PlayerState extends SpriteState {
    maxRotSpeed: number; // Max rotation speed
    minRotSpeed: number; // Min rotation speed

    /**
     * The player made some input and the object state has been updated. This
     * function is called to send these updates to the backend.
     */
    stateUpdate(action: String, hint?: string[]): void;

    /**
     * Function to increase rotation speed.
     */
    deltaRotSpeed(rotSpeed: number): number;
}

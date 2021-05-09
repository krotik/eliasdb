import { EngineEventHandler, PlayerState } from './types';
import { AssetDefinition } from '../backend/asset-loader';
import { AnimationController } from '../game/lib';

// This file contains default values for game display object

/**
 * Default handler for player events.
 */
export abstract class DefaultEngineEventHandler implements EngineEventHandler {
    // Handle when the user presses a key
    //
    public onkeydown(state: PlayerState, e: KeyboardEvent) {
        e = e || window.event;

        let action = 'move';

        switch (e.code) {
            case 'ControlLeft':
            case 'Space':
                state.stateUpdate('fire', []);
                break; // Fire
            case 'ArrowUp':
                state.speed += 0.006;
                state.stateUpdate(action, ['speed']);
                break; // Move forward
            case 'ArrowDown':
                state.speed += -0.008;
                state.stateUpdate(action, ['speed']);
                break; // Move backward
            case 'ArrowRight':
                if (e.ctrlKey || e.shiftKey) {
                    state.strafe = 1; // Strafe right
                    state.stateUpdate(action, ['strafe']);
                } else {
                    state.dir = 1; // Rotate right
                    if (state.rotSpeed < state.maxRotSpeed) {
                        state.rotSpeed = state.deltaRotSpeed(state.rotSpeed);
                        state.stateUpdate(action, ['dir', 'rotSpeed']);
                    } else {
                        state.stateUpdate(action, ['dir']);
                    }
                }
                break;

            case 'ArrowLeft':
                if (e.ctrlKey || e.shiftKey) {
                    state.strafe = -1; // Strafe left
                    state.stateUpdate(action, ['strafe']);
                } else {
                    state.dir = -1; // Rotate left
                    if (state.rotSpeed < state.maxRotSpeed) {
                        state.rotSpeed = state.deltaRotSpeed(state.rotSpeed);
                        state.stateUpdate(action, ['dir', 'rotSpeed']);
                    } else {
                        state.stateUpdate(action, ['dir']);
                    }
                }
                break;
        }

        this.stopBubbleEvent(e);
    }

    // Handle when the user releases a key
    //
    public onkeyup(state: PlayerState, e: KeyboardEvent) {
        e = e || window.event;

        //if (e.code == 'ArrowRight' || e.code == 'ArrowLeft') {
        // Stop rotating and strafing

        state.dir = 0;
        state.strafe = 0;
        state.rotSpeed = state.minRotSpeed;
        state.stateUpdate('stop move', ['dir', 'strafe', 'rotSpeed']);
        //}

        this.stopBubbleEvent(e);
    }

    // Stop the bubbling of an event
    //
    protected stopBubbleEvent(e: KeyboardEvent) {
        e = e || window.event;

        if (e.stopPropagation) {
            e.stopPropagation();
        }
        if (e.cancelBubble !== null) {
            e.cancelBubble = true;
        }
    }
}

/**
 * The player sprite in the world.
 */
export abstract class DefaultEngineOptions {
    public backdrop: CanvasImageSource | null = null;
    public assets: Record<string, AssetDefinition> = {};
    public screenWidth: number = 640;
    public screenHeight: number = 480;
    public screenElementWidth: number = 640;
    public screenElementHeight: number = 480;
}

/**
 * A non-player sprite in the world.
 */
export abstract class DefaultSpriteState {
    public id: string = '';

    public x: number = 20;
    public y: number = 20;

    public kind: string = '';

    public owner: string = '';

    public dim: number = 20;

    public isMoving: boolean = true;

    public displayLoop: boolean = true;

    public dir: number = 0;
    public rot: number = 0;
    public rotSpeed: number = 9 / 100000;

    public speed: number = 0;
    public strafe: number = 0;
    public moveSpeed: number = 0;

    public animation: AnimationController | null = null;

    public setState(state: Record<string, any>): void {
        this.id = state.id || this.id;
        this.x = state.x || this.x;
        this.y = state.y || this.y;
        this.kind = state.kind || this.kind;
        this.owner = state.owner || this.owner;
        this.dim = state.dim || this.dim;
        this.isMoving = state.isMoving || this.isMoving;
        this.displayLoop = state.displayLoop || this.displayLoop;
        this.dir = state.dir || this.dir;
        this.rot = state.rot || this.rot;
        this.rotSpeed = state.rotSpeed || this.rotSpeed;
        this.speed = state.speed || this.speed;
        this.strafe = state.strafe || this.strafe;
        this.moveSpeed = state.moveSpeed || this.moveSpeed;
    }
}

/**
 * The player sprite in the world.
 */
export abstract class DefaultPlayerState extends DefaultSpriteState {
    public maxRotSpeed: number = 9 / 10000;
    public minRotSpeed: number = 1 / 10000;

    public stateUpdate(_action: string, _hint?: string[]): void {}

    public deltaRotSpeed(rotSpeed: number): number {
        return rotSpeed * (1 + 1 / 1000000);
    }
}

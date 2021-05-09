import { EngineOptions, PlayerState, SpriteState } from './types';

/**
 * HTML element id for debug output (will only be used if it is defined)
 */
const debugOutputElementId = 'game-debug-out';

/**
 * Main display controller.
 */
export class MainDisplayController {
    public running: boolean = false;

    protected canvas: HTMLCanvasElement;
    protected debugOutputElement: HTMLElement | null = null;
    protected ctx: CanvasRenderingContext2D;
    protected options: EngineOptions;

    // Runtime state

    protected player: PlayerState = {} as PlayerState;
    protected sprites: SpriteState[] = [];
    protected overlayData: string[] = [];

    private animationFrame: number = 0;

    private isSpectatorMode: boolean = false;
    protected showHelp: boolean = false;

    constructor(canvasElementId: string, options: EngineOptions) {
        this.options = options;

        const canvas = document.getElementById(
            canvasElementId
        ) as HTMLCanvasElement;

        if (canvas === null) {
            throw Error('Canvas element not found');
        }

        this.canvas = canvas;

        const ctx = canvas.getContext('2d');

        if (ctx === null) {
            throw Error('Could not get canvas rendering context');
        }

        this.ctx = ctx;

        this.debugOutputElement = document.getElementById(
            debugOutputElementId
        ) as HTMLCanvasElement;

        this.canvas.width = this.options.screenWidth;
        this.canvas.height = this.options.screenHeight;
        this.canvas.style.width = this.options.screenElementWidth + 'px';
        this.canvas.style.height = this.options.screenElementHeight + 'px';
    }

    /**
     * Register event handlers for the engine.
     */
    public registerEventHandlers(): void {
        document.onkeydown = (e) => {
            // Handle display control

            if (e.code == 'F1') {
                this.showHelp = !this.showHelp;
            }

            this.options.eventHandler.onkeydown(this.player, e);
        };
        document.onkeyup = (e) => {
            this.options.eventHandler.onkeyup(this.player, e);
        };
    }

    /**
     * Deregister event handlers for the engine.
     */
    public deRegisterEventHandlers(): void {
        document.onkeydown = null;
        document.onkeyup = null;
    }

    /**
     * Start the engine.
     */
    public start(playerState: PlayerState): void {
        this.player = playerState;
        this.registerEventHandlers();
        this.isSpectatorMode = false;

        if (!this.running) {
            this.running = true;
            this.drawLoop();
        }
    }

    /**
     * Stop the engine.
     */
    public stop(): void {
        this.running = false;
        this.deRegisterEventHandlers();
        this.options.stopHandler();
    }

    /**
     * Run the engine in spectator mode.
     */
    public spectatorMode(): void {
        this.isSpectatorMode = true;
        this.deRegisterEventHandlers();
    }

    /**
     * Add a sprite to the simulation.
     */
    public addSprite(spriteState: SpriteState): void {
        this.sprites.push(spriteState);
    }

    /**
     * Remove a sprite to the simulation.
     */
    public removeSprite(spriteState: SpriteState): void {
        if (!spriteState) {
            throw new Error('Trying to remove non-existing sprite state');
        }

        this.sprites.splice(this.sprites.indexOf(spriteState), 1);
    }

    setOverlayData(text: string[]) {
        this.overlayData = text;
    }

    /**
     * Print debug information which are cleared with every draw loop.
     */
    public printDebug(s: string): void {
        if (this.debugOutputElement !== null) {
            this.debugOutputElement.innerHTML += s + '<br>';
        }
    }

    /**
     * Loop function which draws the game scene.
     */
    protected drawLoop(): void {
        // Calculate animation frame

        this.animationFrame++;
        this.animationFrame = this.animationFrame % 1000;

        // Clear screen canvas

        if (this.options.backdrop === null) {
            this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        } else {
            this.ctx.drawImage(
                this.options.backdrop,
                0,
                0,
                this.canvas.width,
                this.canvas.height
            );

            // Darken the background image a bit

            this.ctx.save();
            this.ctx.globalAlpha = 0.5;
            this.ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);
            this.ctx.restore();
        }

        // Clear debug element if there is one

        if (this.debugOutputElement !== null) {
            this.debugOutputElement.innerHTML = '';
        }

        let start = new Date().getTime();

        this.drawSprites();

        if (!this.isSpectatorMode) {
            this.drawPlayer();
        }

        // Call external handler

        this.options.drawHandler(this.ctx, this.player, this.sprites);

        if (start !== 0) {
            // Calculate FPS

            let now = new Date().getTime();

            const timeDelta = now - this.lastRenderCycleTime;
            const fps = Math.floor(1000 / timeDelta);

            this.lastRenderCycleTime = now;

            this.printDebug('FPS: ' + fps);
        }

        this.drawInfoOverlay();

        if (this.running) {
            setTimeout(() => {
                this.drawLoop();
            }, 20);
        }
    }

    drawInfoOverlay(): void {
        this.ctx.save();
        this.ctx.font = '10px Arial bold';
        this.ctx.globalAlpha = 0.6;

        let textColor = '#000000';
        this.ctx.fillStyle = '#ffbf00';
        this.ctx.strokeStyle = '#ff8000';

        if (this.isSpectatorMode) {
            let text = [`Player: ${this.player.id}`, ...this.overlayData, ''];

            let centerH = Math.floor(this.options.screenWidth / 2);
            let centerVThird = Math.floor(this.options.screenHeight / 3);
            let heightHalf = Math.floor((text.length * 12) / 2);

            this.drawRoundRect(
                centerH - 200,
                centerVThird - heightHalf,
                400,
                heightHalf * 2 + 40,
                10,
                true,
                true
            );

            this.ctx.fillStyle = textColor;

            this.ctx.font = '20px Arial';
            let gameOverText = 'Game Over';
            this.ctx.fillText(
                gameOverText,
                centerH -
                    Math.floor(this.ctx.measureText(gameOverText).width / 2),
                centerVThird - heightHalf + 30
            );

            this.ctx.font = '10px Arial bold';

            for (let i = 0; i < text.length; i++) {
                let t = text[i].trim();
                let twidth = this.ctx.measureText(t).width;

                this.ctx.fillText(
                    t,
                    centerH - Math.floor(twidth / 2),
                    centerVThird - heightHalf + 50 + i * 12
                );
            }
        } else {
            let text = [`Player: ${this.player.id}`, ...this.overlayData, ''];

            if (this.showHelp) {
                text.push('Controls:');
                text.push('');
                text.push('<cursor left/right>');
                text.push('    Rotate');
                text.push('');
                text.push('<cursor up/down>');
                text.push('    Accelerate/Decelerate');
                text.push('');
                text.push('<space / ctrl>');
                text.push('    Shoot');
                text.push('');
                text.push('<shift>');
                text.push('    Strafe');
            } else {
                text.push('Press F1 for help ...');
            }

            this.drawRoundRect(
                this.options.screenWidth - 170,
                10,
                160,
                13 + text.length * 10,
                10,
                true,
                true
            );

            this.ctx.fillStyle = textColor;

            for (let i = 0; i < text.length; i++) {
                this.ctx.fillText(
                    text[i],
                    this.options.screenWidth - 165,
                    25 + i * 10
                );
            }
        }

        this.ctx.restore();
    }

    private lastRenderCycleTime: number = 0;

    // Draw the player graphics.
    //
    drawPlayer(): void {
        try {
            // Call draw routine in player state

            this.player.draw(this.ctx, this.player, this.options.assets);
            return;
        } catch {}

        // If no specific draw routine is specified then draw a placeholder

        this.ctx.beginPath();
        this.ctx.arc(
            this.player.x,
            this.player.y,
            this.player.dim / 2,
            0,
            2 * Math.PI
        );

        this.ctx.moveTo(this.player.x, this.player.y);
        this.ctx.lineTo(
            this.player.x + Math.cos(this.player.rot) * 20,
            this.player.y + Math.sin(this.player.rot) * 20
        );
        this.ctx.closePath();

        this.ctx.stroke();

        let oldStrokeStyle = this.ctx.strokeStyle;
        let dimHalf = this.player.dim / 2;

        this.ctx.strokeStyle = 'red';
        this.ctx.rect(
            this.player.x - dimHalf,
            this.player.y - dimHalf,
            this.player.dim,
            this.player.dim
        );
        this.ctx.stroke();
        this.ctx.strokeStyle = oldStrokeStyle;
    }

    // Draw sprite graphics
    //
    drawSprites(): void {
        for (let sprite of this.sprites) {
            try {
                // Call draw routine in sprite state

                sprite.draw(this.ctx, sprite, this.options.assets);
                continue;
            } catch {}

            this.ctx.beginPath();
            this.ctx.arc(sprite.x, sprite.y, sprite.dim / 2, 0, 2 * Math.PI);

            this.ctx.moveTo(sprite.x, sprite.y);
            this.ctx.lineTo(
                sprite.x + Math.cos(sprite.rot) * 20,
                sprite.y + Math.sin(sprite.rot) * 20
            );
            this.ctx.closePath();

            this.ctx.stroke();

            let oldStrokeStyle = this.ctx.strokeStyle;
            let dimHalf = sprite.dim / 2;

            this.ctx.strokeStyle = 'red';
            this.ctx.rect(
                sprite.x - dimHalf,
                sprite.y - dimHalf,
                sprite.dim,
                sprite.dim
            );
            this.ctx.stroke();
            this.ctx.strokeStyle = oldStrokeStyle;
        }
    }

    drawRoundRect(
        x: number,
        y: number,
        w: number,
        h: number,
        radius: number = 5,
        fill: boolean = true,
        stroke: boolean = true
    ): void {
        let r = { tl: radius, tr: radius, br: radius, bl: radius };

        this.ctx.beginPath();
        this.ctx.moveTo(x + r.tl, y);
        this.ctx.lineTo(x + w - r.tr, y);
        this.ctx.quadraticCurveTo(x + w, y, x + w, y + r.tr);
        this.ctx.lineTo(x + w, y + h - r.br);
        this.ctx.quadraticCurveTo(x + w, y + h, x + w - r.br, y + h);
        this.ctx.lineTo(x + r.bl, y + h);
        this.ctx.quadraticCurveTo(x, y + h, x, y + h - r.bl);
        this.ctx.lineTo(x, y + r.tl);
        this.ctx.quadraticCurveTo(x, y, x + r.tl, y);
        this.ctx.closePath();

        if (fill) {
            this.ctx.fill();
        }
        if (stroke) {
            this.ctx.stroke();
        }
    }
}

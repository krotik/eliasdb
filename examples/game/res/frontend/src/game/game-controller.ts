import { BackendClient } from '../backend/api-helper';
import { AssetDefinition } from '../backend/asset-loader';
import { EliasDBGraphQLClient } from '../backend/eliasdb-graphql';
import { MainDisplayController } from '../display/engine';
import { PlayerState, SpriteState } from '../display/types';
import { stringToNumber } from '../helper';
import { AnimationController, AnimationStyle, playOneSound } from './lib';
import { Player, Sprite } from './objects';

/**
 * Main game controller.
 */
export class MainGameController {
    protected gameName: string; // Name of the game

    protected playerState: PlayerState;
    protected spriteMap: Record<string, SpriteState>;

    private display: MainDisplayController;
    protected backedClient: BackendClient;
    protected graphQLClient: EliasDBGraphQLClient;
    private assets: Record<string, AssetDefinition>;

    constructor(
        gameName: string,
        playerName: string,
        assets: Record<string, AssetDefinition>,
        display: MainDisplayController,
        backedClient: BackendClient,
        graphQLClient: EliasDBGraphQLClient
    ) {
        this.gameName = gameName;
        this.assets = assets;
        this.display = display;
        this.backedClient = backedClient;
        this.graphQLClient = graphQLClient;

        this.playerState = new Player(gameName, backedClient);
        this.playerState.id = playerName;

        this.spriteMap = {};

        // Retrieve score update

        this.graphQLClient.subscribe(
            `
        subscription {
          score(ascending:score) {
              key,
              score,
          }
        }`,
            (data) => {
                let text = [];
                let scores: any[] = data.data.score.reverse().slice(0, 10);
                let score = 0;

                text.push(``);
                text.push(`Highscores:`);
                text.push(``);

                for (let item of scores) {
                    text.push(`  ${item.key}`);
                    text.push(`          ${item.score}`);
                    if (item.key == this.playerState.id) {
                        score = item.score;
                    }
                }

                text.unshift(`Score: ${score}`);

                this.display.setOverlayData(text);
            }
        );
    }

    public setPlayerWebsocket(ws: WebSocket) {
        (this.playerState as Player).setWebsocket(ws);
    }

    /**
     * Handler which is called every time the backend pushes an update via the
     * websocket for game state updates.
     *
     * @param data Data object from the server.
     */
    public updatePushHandler(data: any): void {
        if (data.payload.audioEvent) {
            let event = data.payload.audioEvent;

            if (event === 'explosion') {
                let explosionType = Math.floor(Math.random() * 5) + 1;
                playOneSound(
                    this.assets[`explosion_00${explosionType}`].audio!
                );
            } else if (event === 'vanish') {
                 playOneSound(
                    this.assets[`vanish_001`].audio!
                );
            } else if (event === 'shot') {
                let player = data.payload.player;
                let shotType = stringToNumber(player, 1, 9);
                playOneSound(this.assets[`shotfired_00${shotType}`].audio!);
            }

            return;
        }

        if (data.payload.toRemovePlayerIds) {
            for (let i of data.payload.toRemovePlayerIds) {
                let entity: SpriteState = this.playerState;
                let callback: () => void;

                if (i === this.playerState.id) {
                    console.log(
                        'Sorry',
                        this.playerState.id,
                        'but you are gone ...'
                    );
                    callback = () => {
                        this.display.spectatorMode();
                    };
                } else {
                    entity = this.spriteMap[i];

                    if (!entity) {
                        continue; // Some removal messages can be send multiple times
                    }
                    callback = () => {
                        delete this.spriteMap[i];
                        this.display.removeSprite(entity);
                    };
                }

                entity.animation = new AnimationController(
                    this.assets['ship_explosion_ani_001'].image!,
                    24,
                    24,
                    AnimationStyle.ForwardAndBackward,
                    3,
                    100,
                    callback
                );
                playOneSound(this.assets['ship_explosion_snd_001'].audio!);
            }
            return;
        }

        if (data.payload.toRemoveSpriteIds) {
            for (let i of data.payload.toRemoveSpriteIds) {
                let entity = this.spriteMap[i];

                if (!entity) {
                    continue; // Some removal messages can be send multiple times
                }
                delete this.spriteMap[i];

                this.display.removeSprite(entity);
            }
            return;
        }

        let obj = data.payload.state as Record<string, any>;

        if (obj.id === this.playerState.id) {
            this.playerState.setState(obj);
        } else {
            let sprite = this.spriteMap[obj.id];

            if (!sprite) {
                sprite = new Sprite();
                sprite.setState(obj);
                this.spriteMap[sprite.id] = sprite;
                this.display.addSprite(sprite);
            }

            sprite.setState(obj);
        }

        // Start the game once we had the first update of all object coordinates

        if (!this.display.running) {
            this.display.start(this.playerState);
        }
    }
}

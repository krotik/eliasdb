import { GameOptions } from './game/objects';

import { MainDisplayController } from './display/engine';

import { GameWorld } from './backend/types';
import { BackendClient, RequestMetod } from './backend/api-helper';
import { EliasDBGraphQLClient } from './backend/eliasdb-graphql';

import { MainGameController } from './game/game-controller';

import { generateRandomName, getURLParams, setURLParam } from './helper';
import {
    AssetDefinition,
    AssetLoader,
    AssetType
} from './backend/asset-loader';
import { playLoop } from './game/lib';

export default {
    generatePlayerName: function () {
        return generateRandomName();
    },

    start: async function (canvasId: string) {
        const host = `${window.location.hostname}:${window.location.port}`;

        let ass = new AssetLoader(host);

        ass.preload(
            [
                {
                    id: 'background_nebular',
                    type: AssetType.ImageAsset,
                    file: 'background_nebular.jpg'
                },
                {
                    id: 'spaceShips_001',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_001.png'
                },
                {
                    id: 'spaceShips_002',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_002.png'
                },
                {
                    id: 'spaceShips_003',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_003.png'
                },
                {
                    id: 'spaceShips_004',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_004.png'
                },
                {
                    id: 'spaceShips_005',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_005.png'
                },
                {
                    id: 'spaceShips_006',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_006.png'
                },
                {
                    id: 'spaceShips_007',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_007.png'
                },
                {
                    id: 'spaceShips_008',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_008.png'
                },
                {
                    id: 'spaceShips_009',
                    type: AssetType.ImageAsset,
                    file: 'spaceShips_009.png'
                },
                {
                    id: 'asteroid_001',
                    type: AssetType.ImageAsset,
                    file: 'asteroid_001.png'
                },
                {
                    id: 'asteroid_002',
                    type: AssetType.ImageAsset,
                    file: 'asteroid_002.png'
                },
                {
                    id: 'shot_001',
                    type: AssetType.ImageAsset,
                    file: 'shot_001.png'
                },
                {
                    id: 'shot_002',
                    type: AssetType.ImageAsset,
                    file: 'shot_002.png'
                },
                {
                    id: 'shot_003',
                    type: AssetType.ImageAsset,
                    file: 'shot_003.png'
                },
                {
                    id: 'explosion_001',
                    type: AssetType.SoundAsset,
                    file: 'explosion_001.mp3'
                },
                {
                    id: 'explosion_002',
                    type: AssetType.SoundAsset,
                    file: 'explosion_002.mp3'
                },
                {
                    id: 'explosion_003',
                    type: AssetType.SoundAsset,
                    file: 'explosion_003.mp3'
                },
                {
                    id: 'explosion_004',
                    type: AssetType.SoundAsset,
                    file: 'explosion_004.mp3'
                },
                {
                    id: 'explosion_005',
                    type: AssetType.SoundAsset,
                    file: 'explosion_005.mp3'
                },
                {
                    id: 'vanish_001',
                    type: AssetType.SoundAsset,
                    file: 'vanish_001.mp3'
                },
                {
                    id: 'shotfired_001',
                    type: AssetType.SoundAsset,
                    file: 'shot_001.mp3'
                },
                {
                    id: 'shotfired_002',
                    type: AssetType.SoundAsset,
                    file: 'shot_002.mp3'
                },
                {
                    id: 'shotfired_003',
                    type: AssetType.SoundAsset,
                    file: 'shot_003.mp3'
                },
                {
                    id: 'shotfired_004',
                    type: AssetType.SoundAsset,
                    file: 'shot_004.mp3'
                },
                {
                    id: 'shotfired_005',
                    type: AssetType.SoundAsset,
                    file: 'shot_005.mp3'
                },
                {
                    id: 'shotfired_006',
                    type: AssetType.SoundAsset,
                    file: 'shot_006.mp3'
                },
                {
                    id: 'shotfired_007',
                    type: AssetType.SoundAsset,
                    file: 'shot_007.mp3'
                },
                {
                    id: 'shotfired_008',
                    type: AssetType.SoundAsset,
                    file: 'shot_008.mp3'
                },
                {
                    id: 'shotfired_009',
                    type: AssetType.SoundAsset,
                    file: 'shot_009.mp3'
                },
                {
                    id: 'ship_explosion_ani_001',
                    type: AssetType.ImageAsset,
                    file: 'ship_explosion_001.png'
                },
                {
                    id: 'ship_explosion_snd_001',
                    type: AssetType.SoundAsset,
                    file: 'ship_explosion_001.mp3'
                },
                {
                    id: 'background_sound',
                    type: AssetType.SoundAsset,
                    file: 'background-sound.mp3'
                }
            ],
            (assets: Record<string, AssetDefinition>) => {
                this.startGame(canvasId, assets);
            }
        );
    },

    startGame: async function (
        canvasId: string,
        assets: Record<string, AssetDefinition>
    ) {
        // Ensure required parameters are present

        let params = getURLParams();

        if (!params.player) {
            setURLParam('player', generateRandomName());
            params = getURLParams();
        }

        if (!params.game) {
            setURLParam('game', 'main');
            params = getURLParams();
        }

        const host = `${window.location.hostname}:${window.location.port}`;
        const gameName = params.game;
        const playerName = params.player;

        // Create backend client to send game specific requests

        const bc = new BackendClient(host);
        const gqlc = new EliasDBGraphQLClient(host);

        // Get option details

        const options = new GameOptions();

        try {
            // Request information about the game world

            let res = await bc.req(
                '/game',
                {
                    gameName
                },
                RequestMetod.Get
            );

            const gm = res.gameworld as GameWorld;

            // Set game world related options

            options.backdrop = null;

            let backdropAsset = assets[gm.backdrop || ''];
            if (backdropAsset) {
                options.backdrop = backdropAsset.image as HTMLImageElement;
            }

            options.assets = assets;
            options.screenWidth = gm.screenWidth;
            options.screenHeight = gm.screenHeight;
            options.screenElementWidth = gm.screenElementWidth;
            options.screenElementHeight = gm.screenElementHeight;
        } catch (e) {
            throw new Error(`Could not get game world information: ${e}`);
        }

        const mdc = new MainDisplayController(canvasId, options);

        try {
            // Create the main game controller

            let gc = new MainGameController(
                gameName,
                playerName,
                assets,
                mdc,
                bc,
                gqlc
            );

            (this as any).GameController = gc;

            // Register websocket for game state updates

            const ws = await bc.createSock(
                '/gamestate',
                {
                    gameName,
                    playerName
                },
                gc.updatePushHandler.bind(gc)
            );

            gc.setPlayerWebsocket(ws);

            await bc.req('/player', {
                player: playerName,
                gameName
            });

            playLoop(assets["background_sound"].audio!)

        } catch (e) {
            throw new Error(`Could not register: ${e}`);
        }
    }
};

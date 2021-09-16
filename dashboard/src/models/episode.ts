export interface ApiEpisode {
    episode: number,
    start: number,
    end: number,
    score: number,
    actions_taken: Map<string, number>
    error: string
    error_message: string
}

export interface Episode {
    episode: number,
    start: Date,
    end: Date,
    reward: number,
    actions_taken: Map<string, number>
    error: string
    error_message: string
}

export const newEpisode = (apiEpisode: ApiEpisode): Episode =>{
    return {
        episode: apiEpisode.episode,
        start: new Date(apiEpisode.start * 1000),
        end: new Date(apiEpisode.end * 1000),
        reward: apiEpisode.score,
        actions_taken: apiEpisode.actions_taken,
        error: apiEpisode.error,
        error_message: apiEpisode.error_message
    } as Episode
}
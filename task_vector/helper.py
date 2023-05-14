from matplotlib import pyplot as plt
from gensim.models import KeyedVectors
from sklearn.decomposition import PCA


def visualise_2d(p, labels, _target_terms, circle_size: float = 2.0, _fig_size=(15, 10)):
    plt.figure(figsize=_fig_size)
    plt.scatter(p[:, 0], p[:, 1], c="lightcoral")
    fig = plt.gcf()
    ax = fig.gca()
    ax.set_aspect("equal")

    for x, y, label in zip(p[:, 0], p[:, 1], labels):
        if label in _target_terms:
            target_zone = plt.Circle((x, y), circle_size, color="mediumturquoise", fill=False)
            ax.add_patch(target_zone)
            plt.annotate(label, xy=(x + 0.05, y + 0.05), color="orangered")
        else:
            plt.annotate(label, xy=(x + 0.05, y + 0.05), color="black")


def dim_reduction(
    _model: KeyedVectors, _target_terms: [str], _topn: int = 20, _n_dim: int = 2
):
    model, similar_terms = generate_word_model(_model, _target_terms, _topn)
    pca = PCA(n_components=_n_dim)
    P = pca.fit_transform(model)
    labels = similar_terms
    return P, labels


def generate_word_model(_model: KeyedVectors, _target_terms: [str], _topn: int = 20):
    target_dict = {
        term: list(
            map(
                lambda x: x[0],
                _model.wv.most_similar_cosmul(positive=term.split(" "), topn=_topn),
            )
        )
        for term in _target_terms
    }
    similar_terms = (
        sum(map(lambda x: [x[0]] + x[1], target_dict.items()), []) + _target_terms
    )
    return _model.wv[similar_terms], similar_terms

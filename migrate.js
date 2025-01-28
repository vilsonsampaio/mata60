const { Client } = require("pg");
const fs = require("node:fs/promises");
const path = require("node:path");

require("dotenv").config();

const pgClient = new Client({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  user: process.env.PG_USER,
  password: process.env.PG_PASS,
  database: process.env.PG_NAME,
  ssl: true,
});

const init = async () => {
  try {
    await pgClient.connect();
    console.log("Conectado ao PostgreSQL.");

    const formatDate = ({ data_criacao, data_atualizacao }) => ({
      data_criacao: { $date: new Date(data_criacao).toISOString() },
      data_atualizacao: data_atualizacao
        ? { $date: new Date(data_atualizacao).toISOString() }
        : null,
    });

    const discos = (
      await pgClient.query(`
        SELECT 
            d.PK_id_disco AS _id, 
            d.titulo, 
            d.codigo, 
            d.ano_lancamento, 
            d.imagem_capa, 
            d.data_criacao, 
            d.data_atualizacao, 
            ARRAY_AGG(DISTINCT a.nome) AS artistas, 
            ARRAY_AGG(DISTINCT g.nome_genero) AS generos 
        FROM 
            tbl_disco d 
            LEFT JOIN tbl_disco_artista da ON d.PK_id_disco = da.FK_id_disco 
            LEFT JOIN tbl_artista a ON da.FK_id_artista = a.PK_id_artista 
            LEFT JOIN tbl_disco_genero dg ON d.PK_id_disco = dg.FK_id_disco 
            LEFT JOIN tbl_genero g ON dg.FK_id_genero = g.PK_id_genero 
        GROUP BY 
            d.PK_id_disco 
        ORDER BY 
            d.pk_id_disco;
    `)
    ).rows.map((disco) => ({
      ...disco,
      ...formatDate(disco),
    }));

    const usuarios = (
      await pgClient.query(`
        SELECT 
            u.PK_id_usuario AS _id,
            u.nome,
            u.email,
            u.senha,
            u.data_criacao,
            u.data_atualizacao,
            w.nome AS wishlist_nome,
            w.data_criacao AS wishlist_data_criacao,
            w.data_atualizacao AS wishlist_data_atualizacao,
            ARRAY_AGG(DISTINCT wd.FK_id_disco) AS wishlist_discos_id
        FROM 
            tbl_usuario u
        LEFT JOIN 
            tbl_wishlist w ON u.PK_id_usuario = w.FK_id_usuario
        LEFT JOIN 
            tbl_wishlist_disco wd ON w.PK_id_wishlist = wd.FK_id_wishlist
        GROUP BY 
            u.PK_id_usuario, w.nome, w.data_criacao, w.data_atualizacao
        ORDER BY _id;
      `)
    ).rows.reduce(
      (
        acc,
        {
          wishlist_nome,
          wishlist_discos_id,
          wishlist_data_criacao,
          wishlist_data_atualizacao,
          ...usuario
        }
      ) => {
        const userIndex = acc.findIndex((user) => user._id === usuario._id);

        if (userIndex !== -1 && wishlist_nome) {
          acc[userIndex].wishlists = acc[userIndex].wishlists || [];
          acc[userIndex].wishlists.push({
            nome: wishlist_nome,
            discos_id: wishlist_discos_id.map(Number),
            data_criacao: { $date: wishlist_data_criacao },
            data_atualizacao: wishlist_data_atualizacao
              ? { $date: wishlist_data_atualizacao }
              : null,
          });

          return acc;
        }

        acc.push({
          ...usuario,
          wishlists: wishlist_nome
            ? [
                {
                  nome: wishlist_nome,
                  discos_id: wishlist_discos_id.map(Number),
                  ...formatDate({
                    data_criacao: wishlist_data_criacao,
                    data_atualizacao: wishlist_data_atualizacao,
                  }),
                },
              ]
            : null,
          ...formatDate(usuario),
        });

        return acc;
      },
      []
    );

    const estoques = (
      await pgClient.query(`
        SELECT 
            e.PK_id_estoque as _id,
            u.PK_id_usuario as id_usuario,
            d.PK_id_disco as id_disco,
            e.tipo,
            e.disponivel_troca,
            e.condicao,
            e.data_criacao,
            e.data_atualizacao,
            a.nota AS nota,
            a.comentario AS comentario,
            a.data_criacao AS avaliacao_data_criacao,
            a.data_atualizacao AS avaliacao_data_atualizacao
        FROM 
            tbl_estoque e
        LEFT JOIN 
            tbl_usuario u ON e.FK_id_usuario = u.PK_id_usuario
        LEFT JOIN 
            tbl_disco d ON e.FK_id_disco = d.PK_id_disco
        LEFT JOIN 
            tbl_avaliacao a ON e.PK_id_estoque = a.FK_id_estoque
        ORDER BY 
            id_usuario, id_disco;
      `)
    ).rows.map(
      ({
        nota,
        comentario,
        avaliacao_data_criacao,
        avaliacao_data_atualizacao,
        ...estoque
      }) => ({
        ...estoque,
        avaliacao: nota
          ? {
              nota,
              comentario,
              ...formatDate({
                data_criacao: avaliacao_data_criacao,
                data_atualizacao: avaliacao_data_atualizacao,
              }),
            }
          : null,
        ...formatDate(estoque),
      })
    );

    await fs.writeFile(
      path.join(__dirname, "data", "discos.json"),
      JSON.stringify(discos, null, 2)
    );
    console.log("Discos exportados para JSON.");

    await fs.writeFile(
      path.join(__dirname, "data", "usuarios.json"),
      JSON.stringify(usuarios, null, 2)
    );
    console.log("Usuários exportados para JSON.");

    await fs.writeFile(
      path.join(__dirname, "data", "estoques.json"),
      JSON.stringify(estoques, null, 2)
    );
    console.log("Estoques exportados para JSON.");
  } catch (err) {
    console.dir(err);
  } finally {
    await pgClient.end();
    console.log("Conexão com PostgreSQL encerrada.");
  }
};

init();
